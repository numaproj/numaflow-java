package io.numaproj.numaflow.reducestreamer;

import akka.actor.ActorRef;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import io.numaproj.numaflow.reducestreamer.model.Message;
import io.numaproj.numaflow.reducestreamer.model.Metadata;
import io.numaproj.numaflow.reducestreamer.model.OutputStreamObserver;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@AllArgsConstructor
class OutputStreamObserverImpl implements OutputStreamObserver {
    private final Metadata md;
    private final ActorRef responseStreamActor;

    @Override
    public void send(Message message) {
        this.responseStreamActor.tell(buildResponse(message), ActorRef.noSender());
    }

    private ActorResponse buildResponse(Message message) {
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
        return new ActorResponse(responseBuilder.build(), false);
    }
}
