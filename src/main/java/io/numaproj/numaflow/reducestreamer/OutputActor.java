package io.numaproj.numaflow.reducestreamer;

import akka.actor.AbstractActor;
import akka.actor.Props;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import io.numaproj.numaflow.reducestreamer.model.Message;
import io.numaproj.numaflow.reducestreamer.model.Metadata;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Output actor is a wrapper around the gRPC output stream.
 * It ensures synchronized calls to the responseObserver onNext() and invokes onComplete at the end of the stream.
 * ALL reduce responses are sent to the response stream actor before getting forwarded to the output gRPC stream.
 * <p>
 * More details about gRPC StreamObserver concurrency: https://grpc.github.io/grpc-java/javadoc/io/grpc/stub/StreamObserver.html
 */
@Slf4j
@AllArgsConstructor
class OutputActor extends AbstractActor {
    StreamObserver<ReduceOuterClass.ReduceResponse> responseObserver;
    Metadata md;

    public static Props props(
            StreamObserver<ReduceOuterClass.ReduceResponse> responseObserver,
            Metadata md) {
        return Props.create(OutputActor.class, responseObserver, md);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.class, this::sendMessage)
                .match(ActorResponse.class, this::sendEOF)
                .build();
    }

    private void sendMessage(Message message) {
        responseObserver.onNext(this.buildResponse(message));
    }

    private void sendEOF(ActorResponse actorResponse) {
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

    private ReduceOuterClass.ReduceResponse buildResponse(Message message) {
        ReduceOuterClass.ReduceResponse.Builder responseBuilder = ReduceOuterClass.ReduceResponse.newBuilder();
        // set the window using the actor metadata.
        responseBuilder.setWindow(ReduceOuterClass.Window.newBuilder()
                .setStart(Timestamp.newBuilder()
                        .setSeconds(this.md.getIntervalWindow().getStartTime().getEpochSecond())
                        .setNanos(this.md.getIntervalWindow().getStartTime().getNano()))
                .setEnd(Timestamp.newBuilder()
                        .setSeconds(this.md.getIntervalWindow().getEndTime().getEpochSecond())
                        .setNanos(this.md.getIntervalWindow().getEndTime().getNano()))
                .setSlot("slot-0").build());
        responseBuilder.setEOF(false);
        // set the result.
        responseBuilder.setResult(ReduceOuterClass.ReduceResponse.Result
                .newBuilder()
                .setValue(ByteString.copyFrom(message.getValue()))
                .addAllKeys(message.getKeys()
                        == null ? new ArrayList<>():Arrays.asList(message.getKeys()))
                .addAllTags(
                        message.getTags() == null ? new ArrayList<>():List.of(message.getTags()))
                .build());
        return responseBuilder.build();
    }
}
