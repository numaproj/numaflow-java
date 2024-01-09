package io.numaproj.numaflow.reducer;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Reduce actor invokes the reducer and returns the result.
 */

@Slf4j
@AllArgsConstructor
class ReduceActor extends AbstractActor {

    private String[] keys;
    private Metadata md;
    private Reducer groupBy;

    public static Props props(String[] keys, Metadata md, Reducer groupBy) {
        return Props.create(ReduceActor.class, keys, md, groupBy);
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder
                .create()
                .match(HandlerDatum.class, this::invokeHandler)
                .match(String.class, this::getResult)
                .build();
    }

    private void invokeHandler(HandlerDatum handlerDatum) {
        this.groupBy.addMessage(keys, handlerDatum, md);
    }

    private void getResult(String eof) {
        MessageList resultMessages = this.groupBy.getOutput(keys, md);
        // send the result back to sender(parent actor)
        getSender().tell(buildDatumListResponse(resultMessages), getSelf());
    }

    private ActorResponse buildDatumListResponse(MessageList messageList) {
        ReduceOuterClass.ReduceResponse.Builder responseBuilder = ReduceOuterClass.ReduceResponse.newBuilder();
        responseBuilder.setWindow(ReduceOuterClass.Window.newBuilder()
                .setStart(Timestamp.newBuilder()
                        .setSeconds(this.md.getIntervalWindow().getStartTime().getEpochSecond())
                        .setNanos(this.md.getIntervalWindow().getStartTime().getNano()))
                .setEnd(Timestamp.newBuilder()
                        .setSeconds(this.md.getIntervalWindow().getEndTime().getEpochSecond())
                        .setNanos(this.md.getIntervalWindow().getEndTime().getNano()))
                .setSlot("slot-0").build());
        // for aligned window, if we start building the response, it means we already reached EOF.
        responseBuilder.setEOF(true);

        ReduceOuterClass.ReduceResponse.Result.Builder resultBuilder = ReduceOuterClass.ReduceResponse.Result.newBuilder();
        messageList.getMessages().forEach(message -> {
            responseBuilder.set
            resultBuilder.setValue(ByteString.copyFrom(message.getValue()))
                    .addAllKeys(message.getKeys() == null ? new ArrayList<>() : Arrays.asList(message.getKeys()))
                    .addAllTags(message.getTags() == null ? new ArrayList<>() : List.of(message.getTags()))
                    .build();
        });


        // set result

        responseBuilder.setResult()

        
        messageList.getMessages().forEach(message -> {



        });
        return new ActorResponse(this.keys, responseBuilder.build());
    }
}

