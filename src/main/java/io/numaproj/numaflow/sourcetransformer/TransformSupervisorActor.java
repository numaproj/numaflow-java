package io.numaproj.numaflow.sourcetransformer;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.AllDeadLetters;
import akka.actor.AllForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import akka.japi.pf.ReceiveBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sourcetransformer.v1.Sourcetransformer;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * TransformSupervisorActor actor is responsible for distributing the messages to actors and handling failure.
 * It creates a TransformerActor for each incoming request and listens to the responses from the TransformerActor.
 * <p>
 * TransformSupervisorActor
 * │
 * ├── Creates TransformerActor instances for each incoming SourceTransformRequest
 * │   │
 * │   ├── TransformerActor 1
 * │   │   ├── Processes SourceTransformRequest
 * │   │   ├── Sends SourceTransformResponse to TransformSupervisorActor
 * │   │   └── Stops itself after processing
 * │   │
 * │   ├── TransformerActor 2
 * │   │   ├── Processes SourceTransformRequest
 * │   │   ├── Sends SourceTransformResponse to TransformSupervisorActor
 * │   │   └── Stops itself after processing
 * │   │
 * ├── Listens to the responses from the TransformerActor instances
 * │   ├── On receiving a SourceTransformResponse, writes the response back to the client
 * │
 * ├── If any TransformerActor fails (throws an exception):
 * │   ├── Sends the exception back to the client
 * │   ├── Initiates a shutdown by completing the CompletableFuture exceptionally
 * │   └── Stops all child actors (AllForOneStrategy)
 */
@Slf4j
class TransformSupervisorActor extends AbstractActor {
    private final SourceTransformer transformer;
    private final StreamObserver<Sourcetransformer.SourceTransformResponse> responseObserver;
    private final CompletableFuture<Void> failureFuture;

    /**
     * Constructor for TransformSupervisorActor.
     *
     * @param transformer The transformer to be used for processing the request.
     * @param responseObserver The StreamObserver to be used for sending the responses.
     * @param failureFuture The CompletableFuture to be completed exceptionally in case of any failure.
     */
    public TransformSupervisorActor(
            SourceTransformer transformer,
            StreamObserver<Sourcetransformer.SourceTransformResponse> responseObserver,
            CompletableFuture<Void> failureFuture) {
        this.transformer = transformer;
        this.responseObserver = responseObserver;
        this.failureFuture = failureFuture;
    }

    /**
     * Creates Props for a TransformSupervisorActor.
     *
     * @param transformer The transformer to be used for processing the request.
     * @param responseObserver The StreamObserver to be used for sending the responses.
     * @param failureFuture The CompletableFuture to be completed exceptionally in case of any failure.
     *
     * @return a Props for creating a TransformSupervisorActor.
     */
    public static Props props(
            SourceTransformer transformer,
            StreamObserver<Sourcetransformer.SourceTransformResponse> responseObserver,
            CompletableFuture<Void> failureFuture) {
        return Props.create(
                TransformSupervisorActor.class,
                transformer,
                responseObserver,
                failureFuture);
    }

    /**
     * Defines the behavior of the actor when it is restarted due to an exception.
     *
     * @param reason The exception that caused the restart.
     * @param message The message that was being processed when the exception was thrown.
     */
    @Override
    public void preRestart(Throwable reason, Optional<Object> message) {
        log.debug("supervisor pre restart was executed");
        failureFuture.completeExceptionally(reason);
        responseObserver.onError(Status.UNKNOWN
                .withDescription(reason.getMessage())
                .withCause(reason)
                .asException());
        Service.transformerActorSystem.stop(getSelf());
    }

    /**
     * Defines the behavior of the actor when it is stopped.
     */
    @Override
    public void postStop() {
        log.debug("post stop of supervisor executed - {}", getSelf().toString());
    }

    /**
     * Defines the initial actor behavior, i.e., what it does on startup and when it begins to process messages.
     *
     * @return a Receive object defining the initial behavior of the actor.
     */
    @Override
    public Receive createReceive() {
        return ReceiveBuilder
                .create()
                .match(Sourcetransformer.SourceTransformRequest.class, this::processRequest)
                .match(Sourcetransformer.SourceTransformResponse.class, this::sendResponse)
                .match(Exception.class, this::handleFailure)
                .match(AllDeadLetters.class, this::handleDeadLetters)
                .match(String.class, eof -> responseObserver.onCompleted())
                .build();
    }

    /**
     * Handles any exception that occurs during the processing of the SourceTransformRequest.
     *
     * @param e The exception to be handled.
     */
    private void handleFailure(Exception e) {
        responseObserver.onError(Status.UNKNOWN
                .withDescription(e.getMessage())
                .withCause(e)
                .asException());
        failureFuture.completeExceptionally(e);
    }

    /**
     * Sends the SourceTransformResponse back to the client.
     *
     * @param transformResponse The SourceTransformResponse to be sent.
     */
    private void sendResponse(Sourcetransformer.SourceTransformResponse transformResponse) {
        responseObserver.onNext(transformResponse);
    }

    /**
     * Processes the SourceTransformRequest by creating a TransformerActor and sending the request to it.
     *
     * @param transformRequest The SourceTransformRequest to be processed.
     */
    private void processRequest(Sourcetransformer.SourceTransformRequest transformRequest) {
        // Create a TransformerActor for each incoming request.
        ActorRef transformerActor = getContext()
                .actorOf(TransformerActor.props(
                        transformer));

        // Send the message to the TransformerActor.
        transformerActor.tell(transformRequest, getSelf());
    }

    /**
     * Handles any dead letters that occur during the processing of the SourceTransformRequest.
     *
     * @param deadLetter The dead letter to be handled.
     */
    private void handleDeadLetters(AllDeadLetters deadLetter) {
        log.debug("got a dead letter, stopping the execution");
        responseObserver.onError(Status.UNKNOWN.withDescription("dead letters").asException());
        failureFuture.completeExceptionally(new Throwable("dead letters"));
        getContext().getSystem().stop(getSelf());
    }

    /**
     * Defines the supervisor strategy for the actor.
     *
     * @return the supervisor strategy for the actor.
     */
    @Override
    public SupervisorStrategy supervisorStrategy() {
        // we want to stop all child actors in case of any exception
        return new AllForOneStrategy(
                DeciderBuilder
                        .match(Exception.class, e -> {
                            failureFuture.completeExceptionally(e);
                            responseObserver.onError(Status.UNKNOWN
                                    .withDescription(e.getMessage())
                                    .withCause(e)
                                    .asException());
                            return SupervisorStrategy.stop();
                        })
                        .build()
        );
    }
}
