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
 * ResponseStreamActor is dedicated to ensure synchronized calls to the responseObserver onNext().
 * ALL the responses are sent to ResponseStreamActor before getting sent to output gRPC stream.
 * <p>
 * More details about gRPC StreamObserver concurrency: https://grpc.github.io/grpc-java/javadoc/io/grpc/stub/StreamObserver.html
 */
@Slf4j
@AllArgsConstructor
public class ResponseStreamActor extends AbstractActor {
    StreamObserver<ReduceOuterClass.ReduceResponse> responseObserver;
    Metadata md;

    public static Props props(
            StreamObserver<ReduceOuterClass.ReduceResponse> responseObserver,
            Metadata md) {
        return Props.create(ResponseStreamActor.class, responseObserver, md);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.class, this::sendMessage)
                .match(ActorResponse.class, this::sendEOF)
                .build();
    }

    private void sendMessage(Message message) {
        // Synchronized access to the output stream
        synchronized (responseObserver) {
            responseObserver.onNext(this.buildResponse(message));
        }
    }

    private void sendEOF(ActorResponse actorResponse) {
        if (actorResponse.getType() != ActorResponseType.EOF_RESPONSE) {
            throw new RuntimeException(
                    "Unexpected behavior - Response Stream actor received a non-eof response. Response type is: "
                            + actorResponse.getType());
        }
        // Synchronized access to the output stream
        synchronized (responseObserver) {
            responseObserver.onNext(actorResponse.getResponse());
        }
        // After the EOF response gets sent to gRPC output stream,
        // tell the supervisor that the actor is ready to be cleaned up.
        getSender().tell(
                new ActorResponse(
                        actorResponse.getResponse(),
                        ActorResponseType.READY_FOR_CLEAN_UP_SIGNAL),
                getSelf());
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
