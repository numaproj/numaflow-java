package io.numaproj.numaflow.mapper;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.AllDeadLetters;
import akka.actor.AllForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import akka.japi.pf.ReceiveBuilder;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.map.v1.MapOuterClass;
import io.numaproj.numaflow.shared.ExceptionUtils;
import io.numaproj.numaflow.shared.InputStreamError;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MapSupervisorActor actor is responsible for distributing the messages to actors and handling failure.
 * It creates a MapperActor for each incoming request and listens to the responses from the MapperActor.
 * <p>
 * MapSupervisorActor
 * │
 * ├── Creates MapperActor instances for each incoming MapRequest
 * │   │
 * │   ├── MapperActor 1
 * │   │   ├── Processes MapRequest
 * │   │   ├── Sends MapResponse to MapSupervisorActor
 * │   │   └── Stops itself after processing
 * │   │
 * │   ├── MapperActor 2
 * │   │   ├── Processes MapRequest
 * │   │   ├── Sends MapResponse to MapSupervisorActor
 * │   │   └── Stops itself after processing
 * │   │
 * ├── Listens to the responses from the MapperActor instances
 * │   ├── On receiving a MapResponse, writes the response back to the client
 * │
 * ├── If any MapperActor fails (throws an exception):
 * │   ├── Sends the exception back to the client
 * │   ├── Initiates a shutdown by completing the CompletableFuture exceptionally
 * │   └── Stops all child actors (AllForOneStrategy)
 */
@Slf4j
class MapSupervisorActor extends AbstractActor {
    private final Mapper mapper;
    private final StreamObserver<MapOuterClass.MapResponse> responseObserver;
    private final CompletableFuture<Void> shutdownSignal;
    private final AtomicBoolean streamClosed = new AtomicBoolean(false);
    private boolean inputCompleted;
    private int activeMapperCount;
    private Exception userException;

    public MapSupervisorActor(
            Mapper mapper,
            StreamObserver<MapOuterClass.MapResponse> responseObserver,
            CompletableFuture<Void> failureFuture) {
        this.mapper = mapper;
        this.responseObserver = responseObserver;
        this.shutdownSignal = failureFuture;
        this.inputCompleted = false;
        this.userException = null;
        this.activeMapperCount = 0;
    }

    public static Props props(
            Mapper mapper,
            StreamObserver<MapOuterClass.MapResponse> responseObserver,
            CompletableFuture<Void> shutdownSignal) {
        return Props.create(MapSupervisorActor.class, mapper, responseObserver, shutdownSignal);
    }

    @Override
    public void preRestart(Throwable reason, Optional<Object> message) {
        getContext()
                .getSystem()
                .log()
                .warning("supervisor pre restart was executed due to: {}", reason.getMessage());
        sendError(Status.INTERNAL
                .withDescription(reason.getMessage())
                .withCause(reason)
                .asException());
        Service.mapperActorSystem.stop(getSelf());
        shutdownSignal.completeExceptionally(reason);
    }

    @Override
    public void postStop() {
        log.debug("post stop of supervisor executed - {}", getSelf().toString());
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder
                .create()
                .match(MapOuterClass.MapRequest.class, this::processRequest)
                .match(MapOuterClass.MapResponse.class, this::sendResponse)
                .match(InputStreamError.class, this::handleInputStreamError)
                .match(Exception.class, this::handleFailure)
                .match(AllDeadLetters.class, this::handleDeadLetters)
                .match(String.class, eof -> handleInputCompleted())
                .build();
    }

    private void handleFailure(Exception e) {
        log.error("Encountered error in mapFn", e);
        if (userException == null) {
            userException = e;
            // only send the very first exception to the client
            // one exception should trigger a container restart
            // Build gRPC Status
            com.google.rpc.Status status = ExceptionUtils.buildStatusFromUserException(e);
            sendError(StatusProto.toStatusRuntimeException(status));
        }
        activeMapperCount--;
        finishIfDrained();
    }

    private void sendResponse(MapOuterClass.MapResponse mapResponse) {
        try {
            if (!streamClosed.get()) {
                responseObserver.onNext(mapResponse);
            }
        } catch (RuntimeException e) {
            handleResponseObserverFailure(e);
        } finally {
            activeMapperCount--;
            finishIfDrained();
        }
    }

    private void processRequest(MapOuterClass.MapRequest mapRequest) {
        if (userException != null) {
            log.info("a previous mapper actor failed, not processing any more requests");
            return;
        }

        // Create a MapperActor for each incoming request.
        ActorRef mapperActor = getContext()
                .actorOf(MapperActor.props(
                        mapper));

        // Send the message to the MapperActor.
        mapperActor.tell(mapRequest, getSelf());
        activeMapperCount++;
    }

    private void handleInputStreamError(InputStreamError error) {
        log.error("inbound request stream error, stopping mapper supervisor", error.getCause());
        streamClosed.set(true);
        getContext().getSystem().stop(getSelf());
        shutdownSignal.completeExceptionally(error.getCause());
    }

    // if we see dead letters, we need to stop the execution and exit
    // to make sure no messages are lost
    private void handleDeadLetters(AllDeadLetters deadLetter) {
        log.error("got a dead letter, stopping the execution");
        sendError(Status.INTERNAL.withDescription("dead letters").asException());
        getContext().getSystem().stop(getSelf());
        shutdownSignal.completeExceptionally(new Throwable("dead letters"));
    }

    private void handleInputCompleted() {
        inputCompleted = true;
        finishIfDrained();
    }

    // EOF and failures can arrive while child actors are still processing.
    // Only finish the stream once all started child actors have replied or failed.
    private void finishIfDrained() {
        if (activeMapperCount != 0) {
            return;
        }
        if (userException != null) {
            getContext().getSystem().stop(getSelf());
            shutdownSignal.completeExceptionally(userException);
            return;
        }
        if (inputCompleted) {
            completeResponse();
        }
    }

    private void completeResponse() {
        if (streamClosed.compareAndSet(false, true)) {
            try {
                responseObserver.onCompleted();
            } catch (RuntimeException e) {
                handleResponseObserverFailure(e);
            } finally {
                getContext().getSystem().stop(getSelf());
            }
        }
    }

    private void sendError(Throwable throwable) {
        if (streamClosed.compareAndSet(false, true)) {
            try {
                responseObserver.onError(throwable);
            } catch (RuntimeException e) {
                handleResponseObserverFailure(e);
            }
        }
    }

    private void handleResponseObserverFailure(RuntimeException e) {
        log.warn("response stream is already closed; stopping mapper supervisor", e);
        streamClosed.set(true);
        getContext().getSystem().stop(getSelf());
        shutdownSignal.completeExceptionally(e);
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        // we want to stop all child actors in case of any exception
        return new AllForOneStrategy(
                DeciderBuilder
                        .match(Exception.class, e -> {
                            sendError(Status.INTERNAL
                                    .withDescription(e.getMessage())
                                    .withCause(e)
                                    .asException());
                            shutdownSignal.completeExceptionally(e);
                            return SupervisorStrategy.stop();
                        })
                        .build()
        );
    }
}
