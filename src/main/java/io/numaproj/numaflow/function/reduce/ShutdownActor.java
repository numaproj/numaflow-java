package io.numaproj.numaflow.function.reduce;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.function.FunctionService;
import io.numaproj.numaflow.function.v1.Udfunction;
import lombok.AllArgsConstructor;

import java.util.concurrent.CompletableFuture;

/**
 * Shutdown actor, listens to exceptions and handles shutdown.
 */


@AllArgsConstructor
public class ShutdownActor extends AbstractActor {
    private StreamObserver<Udfunction.DatumList> responseObserver;
    private final CompletableFuture<Void> failureFuture;

    public static Props props(StreamObserver<Udfunction.DatumList> responseObserver, CompletableFuture<Void> failureFuture) {
        return Props.create(ShutdownActor.class, responseObserver, failureFuture);
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder
                .create()
                .match(Throwable.class, this::shutdown)
                .match(String.class, this::completedSuccessfully)
                .build();
    }

    /*
         complete the future with exception so that the exception will be thrown
         indicate that same to response observer.

     */
    private void shutdown(Throwable throwable) {
        failureFuture.completeExceptionally(throwable);
        responseObserver.onError(throwable);
        FunctionService.actorSystem.terminate();
    }

    // if there are no exceptions, complete the future without exception.
    private void completedSuccessfully(String eof) {
        failureFuture.complete(null);
    }
}
