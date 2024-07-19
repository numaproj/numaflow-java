package io.numaproj.numaflow.batchmapper;

import akka.actor.AbstractActor;
import akka.actor.AllDeadLetters;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Slf4j
@AllArgsConstructor
public class BatchMapShutdownActor extends AbstractActor {
    private final CompletableFuture<Void> failureFuture;

    public static Props props(
            CompletableFuture<Void> failureFuture) {
        return Props.create(BatchMapShutdownActor.class, failureFuture);
    }

    @Override
    public void preRestart(Throwable reason, Optional<Object> message) {
        failureFuture.completeExceptionally(reason);
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder
                .create()
                .match(Throwable.class, this::shutdown)
                .match(String.class, this::completedSuccessfully)
                .match(AllDeadLetters.class, this::handleDeadLetters)
                .build();
    }

    /*
        complete the future with exception so that the exception will be thrown
        indicate that same to response observer. This will close the channel and force a retry from the client
    */
    private void shutdown(Throwable throwable) {
        log.info("got a shut down exception");
        failureFuture.completeExceptionally(throwable);
    }

    // if there are no exceptions, complete the future without exception.
    private void completedSuccessfully(String eof) {
        log.info("completed successfully of shutdown executed");
        failureFuture.complete(null);
        // if all the actors completed successfully, we can stop the shutdown actor.
        getContext().getSystem().stop(getSelf());
    }

    // if we see dead letters, we need to stop the execution and exit
    // to make sure no messages are lost
    private void handleDeadLetters(AllDeadLetters deadLetter) {
        log.info("got a dead letter, stopping the execution");
        failureFuture.completeExceptionally(new Throwable("dead letters"));
        getContext().getSystem().stop(getSelf());
    }
}
