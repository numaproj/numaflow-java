package io.numaproj.numaflow.function;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.google.protobuf.ByteString;
import io.numaproj.numaflow.function.handlers.ReduceHandler;
import io.numaproj.numaflow.function.interfaces.Metadata;
import io.numaproj.numaflow.function.types.MessageList;
import io.numaproj.numaflow.function.v1.Udfunction;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Reduce actor invokes the user defined code and returns the result.
 */

@Slf4j
@AllArgsConstructor
class ReduceActor extends AbstractActor {

    private String[] keys;
    private Metadata md;
    private ReduceHandler groupBy;

    public static Props props(String[] keys, Metadata md, ReduceHandler groupBy) {
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
        Udfunction.DatumResponseList.Builder datumListBuilder = Udfunction.DatumResponseList.newBuilder();
        messageList.getMessages().forEach(message -> {
            datumListBuilder.addElements(Udfunction.DatumResponse.newBuilder()
                    .setValue(ByteString.copyFrom(message.getValue()))
                    .addAllKeys(message.getKeys() == null ? new ArrayList<>() : Arrays.asList(
                            message.getKeys()))
                    .addAllTags(message.getTags() == null ? new ArrayList<>() : List.of(
                            message.getTags()))
                    .build());

        });
        return new ActorResponse(this.keys, datumListBuilder.build());
    }

}

