package io.numaproj.numaflow.sessionreducer;

import akka.actor.AbstractActor;
import akka.actor.Props;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
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
    StreamObserver<ReduceOuterClass.ReduceResponse> responseObserver;

    public static Props props(
            StreamObserver<ReduceOuterClass.ReduceResponse> responseObserver) {
        return Props.create(OutputActor.class, responseObserver);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ActorResponse.class, this::handleResponse)
                .build();
    }

    private void handleResponse(ActorResponse actorResponse) {
        if (actorResponse.isLast()) {
            // send the very last response.
            responseObserver.onNext(actorResponse.getResponse());
            // close the output stream.
            responseObserver.onCompleted();
            // stop the AKKA system right after we close the output stream.
            // note: could make more sense if the supervisor actor stops the system,
            // but it requires an extra tell.
            getContext().getSystem().stop(getSender());
        } else {
            responseObserver.onNext(actorResponse.getResponse());
        }
    }
}
