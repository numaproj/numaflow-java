package io.numaproj.numaflow.mapstreamer;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.AllDeadLetters;
import akka.actor.AllForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.map.v1.MapOuterClass;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

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
 * ├── Listens to responses and errors from the MapStreamerActor instances
 * │   ├── On receiving a result, forwards it to the gRPC client via StreamObserver
 * │   ├── On error, forwards the error to the gRPC client and initiates shutdown
 * │
 * ├── Uses AllForOneStrategy for supervising children actors.
 * │   ├── On any MapperActor failure, stops all child actors and resumes by restarting.
 */
@Slf4j
class MapStreamSupervisorActor extends AbstractActor {

    private final MapStreamer mapStreamer;
    private final StreamObserver<MapOuterClass.MapResponse> responseObserver;
    private final CompletableFuture<Void> shutdownSignal;
    private int activeMapperCount;
    private Exception userException;

    public MapStreamSupervisorActor(
            MapStreamer mapStreamer,
            StreamObserver<MapOuterClass.MapResponse> responseObserver,
            CompletableFuture<Void> failureFuture) {
        this.mapStreamer = mapStreamer;
        this.responseObserver = responseObserver;
        this.shutdownSignal = failureFuture;
        this.userException = null;
        this.activeMapperCount = 0;
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
        shutdownSignal.completeExceptionally(reason);
        synchronized (responseObserver) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(reason.getMessage())
                    .withCause(reason)
                    .asException());
        }
    }

    // if we see dead letters, we need to stop the execution and exit
    // to make sure no messages are lost
    private void handleDeadLetters(AllDeadLetters deadLetter) {
        log.error("got a dead letter, stopping the execution");
        responseObserver.onError(Status.INTERNAL.withDescription("dead letters").asException());
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
                .match(Exception.class, this::handleFailure)
                .match(AllDeadLetters.class, this::handleDeadLetters)
                .build();
    }

    private void handleFailure(Exception e) {
        getContext().getSystem().log().error("Encountered error in mapStreamFn", e);
        if (userException == null) {
            userException = e;
            synchronized (responseObserver) {
                responseObserver.onError(Status.INTERNAL
                        .withDescription(e.getMessage())
                        .withCause(e)
                        .asException());
            }
        }
        activeMapperCount--;
    }

    private void sendResponse(MapOuterClass.MapResponse mapResponse) {
        synchronized (responseObserver) {
            responseObserver.onNext(mapResponse);
        }
        activeMapperCount--;
    }

    private void processRequest(MapOuterClass.MapRequest mapRequest) {
        if (userException != null) {
            getContext()
                    .getSystem()
                    .log()
                    .info("Previous mapper actor failed, not processing further requests");
            if (activeMapperCount == 0) {
                getContext().getSystem().log().info("No active mapper actors, shutting down");
                getContext().getSystem().terminate();
                shutdownSignal.completeExceptionally(userException);
            }
            return;
        }

        ActorRef mapStreamerActor = getContext().actorOf(MapStreamerActor.props(
                mapStreamer));
        mapStreamerActor.tell(mapRequest, getSelf());
        activeMapperCount++;
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return new AllForOneStrategy(
                DeciderBuilder.match(Exception.class, e -> {
                    shutdownSignal.completeExceptionally(e);
                    responseObserver.onError(Status.INTERNAL
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asException());
                    return SupervisorStrategy.stop();
                }).build()
        );
    }
}

