package io.numaproj.numaflow.accumulator;

import akka.actor.AbstractActor;
import akka.actor.Props;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.accumulator.v1.AccumulatorOuterClass;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Output actor is a wrapper around the gRPC output stream.
 * It ensures synchronized calls to the responseObserver onNext() and invokes onComplete at the end of the stream.
 * ALL reduce responses are sent to the output actor before getting forwarded to the output gRPC stream.
 * <p>
 * More details about gRPC StreamObserver concurrency: https://grpc.github.io/grpc-java/javadoc/io/grpc/stub/StreamObserver.html
 */
@Slf4j
@AllArgsConstructor
class OutputActor extends AbstractActor {
    StreamObserver<AccumulatorOuterClass.AccumulatorResponse> responseObserver;

    public static Props props(
            StreamObserver<AccumulatorOuterClass.AccumulatorResponse> responseObserver) {
        return Props.create(
                OutputActor.class,
                responseObserver);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(AccumulatorOuterClass.AccumulatorResponse.class, this::handleResponse)
                .match(String.class, this::handleEOF)
                .build();
    }

    private void handleResponse(AccumulatorOuterClass.AccumulatorResponse response) {
        responseObserver.onNext(response);
    }

    private void handleEOF(String eof) {
        responseObserver.onCompleted();
    }
}
