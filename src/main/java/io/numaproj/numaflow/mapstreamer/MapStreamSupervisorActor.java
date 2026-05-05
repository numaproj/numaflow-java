package io.numaproj.numaflow.mapstreamer;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.AllDeadLetters;
import akka.actor.AllForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
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
 * MapStreamSupervisorActor is responsible for managing MapStreamerActor instances and handling failures.
 * It creates a MapStreamerActor for each incoming MapRequest and listens to their responses.
 * <p>
 * MapStreamSupervisorActor
 * │
 * ├── Creates MapStreamerActor instances for each incoming MapRequest
 * │   │
 * │   ├── MapStreamerActor 1
 * │   │   ├── Processes MapRequest
 * │   │   ├── Sends results/errors to MapStreamSupervisorActor
 * │   │   └── Stops itself after processing
 * │   │
 * │   ├── MapStreamerActor 2
 * │   │   ├── Processes MapRequest
 * │   │   ├── Sends results/errors to MapStreamSupervisorActor
 * │   │   └── Stops itself after processing
 * │   │
 * ├── Listens to responses and errors from the MapStreamerActor instances➝➝
 * │   ├── On receiving a result, forwards it to the gRPC client via StreamObserver
 * │   ├── On error, forwards the error to the gRPC client and initiates shutdown
 * │
 * ├── Uses AllForOneStrategy for supervising children actors.
 * │   ├── On any MapStreamerActor failure, stops all child actors and resumes by restarting.
 * <p>
 * Note: After all the output messages are streamed to the client, we send an EOF message to
 * indicate the end of the stream to the client.
 */
@Slf4j
class MapStreamSupervisorActor extends AbstractActor {

    private final MapStreamer mapStreamer;
    private final StreamObserver<MapOuterClass.MapResponse> responseObserver;
    private final CompletableFuture<Void> shutdownSignal;
    private final AtomicBoolean streamClosed = new AtomicBoolean(false);
    private boolean inputCompleted;
    private int activeMapStreamersCount;
    private Exception userException;

    public MapStreamSupervisorActor(
            MapStreamer mapStreamer,
            StreamObserver<MapOuterClass.MapResponse> responseObserver,
            CompletableFuture<Void> failureFuture) {
        this.mapStreamer = mapStreamer;
        this.responseObserver = responseObserver;
        this.shutdownSignal = failureFuture;
        this.inputCompleted = false;
        this.userException = null;
        this.activeMapStreamersCount = 0;
    }

    public static Props props(
            MapStreamer mapStreamer,
            StreamObserver<MapOuterClass.MapResponse> responseObserver,
            CompletableFuture<Void> shutdownSignal) {
        return Props.create(
                MapStreamSupervisorActor.class,
                () -> new MapStreamSupervisorActor(mapStreamer, responseObserver, shutdownSignal));
    }

    @Override
    public void preRestart(Throwable reason, Optional<Object> message) {
        getContext()
                .getSystem()
                .log()
                .warning("supervisor pre restart due to: {}", reason.getMessage());
        sendError(Status.INTERNAL
                .withDescription(reason.getMessage())
                .withCause(reason)
                .asException());
        getContext().getSystem().stop(getSelf());
        shutdownSignal.completeExceptionally(reason);
    }

    private void handleInputStreamError(InputStreamError error) {
        log.error("inbound request stream error, stopping map-stream supervisor", error.getCause());
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

    @Override
    public void postStop() {
        getContext().getSystem().log().debug("post stop - {}", getSelf().toString());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(MapOuterClass.MapRequest.class, this::processRequest)
                .match(MapOuterClass.MapResponse.class, this::sendResponse)
                .match(InputStreamError.class, this::handleInputStreamError)
                .match(Exception.class, this::handleFailure)
                .match(AllDeadLetters.class, this::handleDeadLetters)
                .match(String.class, eof -> handleInputCompleted())
                .build();
    }

    private void handleFailure(Exception e) {
        getContext().getSystem().log().error("Encountered error in mapStreamFn {}", e);
        if (userException == null) {
            userException = e;
            com.google.rpc.Status status = ExceptionUtils.buildStatusFromUserException(e);
            sendError(StatusProto.toStatusRuntimeException(status));
        }
        activeMapStreamersCount--;
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
            activeMapStreamersCount--;
            finishIfDrained();
        }
    }

    private void processRequest(MapOuterClass.MapRequest mapRequest) {
        if (userException != null) {
            getContext().getSystem().log().info("Previous mapStreamer actor failed, not processing further requests");
            return;
        }

        ActorRef mapStreamerActor = getContext().actorOf(MapStreamerActor.props(
                mapStreamer));
        mapStreamerActor.tell(mapRequest, getSelf());
        activeMapStreamersCount++;
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return new AllForOneStrategy(
                DeciderBuilder.match(Exception.class, e -> {
                    sendError(Status.INTERNAL
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asException());
                    shutdownSignal.completeExceptionally(e);
                    return SupervisorStrategy.stop();
                }).build()
        );
    }

    private void handleInputCompleted() {
        inputCompleted = true;
        finishIfDrained();
    }

    // EOF and failures can arrive while child actors are still processing.
    // Only finish the stream once all started child actors have replied or failed.
    private void finishIfDrained() {
        if (activeMapStreamersCount != 0) {
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
        log.warn("response stream is already closed; stopping map-stream supervisor", e);
        streamClosed.set(true);
        getContext().getSystem().stop(getSelf());
        shutdownSignal.completeExceptionally(e);
    }
}

