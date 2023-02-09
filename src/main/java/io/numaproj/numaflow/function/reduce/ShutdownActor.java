package io.numaproj.numaflow.function.reduce;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.function.FunctionService;
import io.numaproj.numaflow.function.v1.Udfunction;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ShutdownActor extends AbstractActor {
    private StreamObserver<Udfunction.DatumList> responseObserver;

    public static Props props(StreamObserver<Udfunction.DatumList> responseObserver) {
        return Props.create(ShutdownActor.class, responseObserver);
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder
                .create()
                .match(Throwable.class, this::shutdown)
                .build();
    }

    private void shutdown(Throwable throwable) {
        responseObserver.onError(throwable);
        FunctionService.actorSystem.terminate();
    }
}
