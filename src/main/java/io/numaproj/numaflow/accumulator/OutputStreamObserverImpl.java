package io.numaproj.numaflow.accumulator;

import akka.actor.ActorRef;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.numaproj.numaflow.accumulator.model.Message;
import io.numaproj.numaflow.accumulator.model.OutputStreamObserver;
import io.numaproj.numaflow.accumulator.v1.AccumulatorOuterClass;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;

class OutputStreamObserverImpl implements OutputStreamObserver {
    private final ActorRef outputActor;
    private final AccumulatorOuterClass.KeyedWindow keyedWindow;
    private final Instant latestWatermark = Instant.ofEpochMilli(-1);

    public OutputStreamObserverImpl(
            ActorRef outputActor,
            AccumulatorOuterClass.KeyedWindow keyedWindow) {
        this.outputActor = outputActor;
        this.keyedWindow = keyedWindow;
    }

    private AccumulatorOuterClass.AccumulatorResponse buildResponse(Message message) {
        AccumulatorOuterClass.AccumulatorResponse.Builder responseBuilder = AccumulatorOuterClass.AccumulatorResponse.newBuilder();
        // set the window using the actor metadata.
        responseBuilder.setWindow(AccumulatorOuterClass.KeyedWindow.newBuilder()
                .setStart(Timestamp.newBuilder()
                        .setSeconds(0)
                        .setNanos(0))
                .setEnd(Timestamp.newBuilder()
                        .setSeconds(this.latestWatermark.getEpochSecond())
                        .setNanos(this.latestWatermark.getNano()))
                .setSlot("slot-0")
                .addAllKeys(this.keyedWindow.getKeysList())
                .build());

        responseBuilder.setEOF(false);
        // set the result.
        return responseBuilder
                .setPayload(AccumulatorOuterClass.Payload
                        .newBuilder()
                        .setValue(ByteString.copyFrom(message.getValue()))
                        .setEventTime(Timestamp
                                .newBuilder()
                                .setSeconds(message.getEventTime().getEpochSecond())
                                .setNanos(message.getEventTime().getNano()))
                        .setWatermark(Timestamp
                                .newBuilder()
                                .setSeconds(message.getWatermark().getEpochSecond())
                                .setNanos(message.getWatermark().getNano()))
                        .putAllHeaders(message.getHeaders())
                        .addAllKeys(message.getKeys()
                                == null ? new ArrayList<>() : Arrays.asList(message.getKeys()))
                        .setId(message.getId())
                        .build())
                .addAllTags(
                        message.getTags() == null ? new ArrayList<>() : Arrays.asList(message.getTags()))
                .build();
    }

    @Override
    public void send(Message message) {
        this.outputActor.tell(buildResponse(message), ActorRef.noSender());
    }
}
