package io.numaproj.numaflow.sessionreducer;

import akka.actor.ActorRef;
import com.google.protobuf.ByteString;
import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;
import io.numaproj.numaflow.sessionreducer.model.Message;
import io.numaproj.numaflow.sessionreducer.model.OutputStreamObserver;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@AllArgsConstructor
class OutputStreamObserverImpl implements OutputStreamObserver {
    private final ActorRef responseStreamActor;

    @Override
    public void send(Message message) {
        this.responseStreamActor.tell(buildResponse(message), ActorRef.noSender());
    }

    private ActorResponse buildResponse(Message message) {
        Sessionreduce.SessionReduceResponse.Builder responseBuilder = Sessionreduce.SessionReduceResponse.newBuilder();
        // set the window using the actor metadata.
        /*
        responseBuilder.setWindow(ReduceOuterClass.Window.newBuilder()
                .setStart(Timestamp.newBuilder()
                        .setSeconds(this.md.getIntervalWindow().getStartTime().getEpochSecond())
                        .setNanos(this.md.getIntervalWindow().getStartTime().getNano()))
                .setEnd(Timestamp.newBuilder()
                        .setSeconds(this.md.getIntervalWindow().getEndTime().getEpochSecond())
                        .setNanos(this.md.getIntervalWindow().getEndTime().getNano()))
                .setSlot("slot-0").build());
         */
        responseBuilder.setEOF(false);
        // set the result.
        responseBuilder.setResult(Sessionreduce.SessionReduceResponse.Result
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
