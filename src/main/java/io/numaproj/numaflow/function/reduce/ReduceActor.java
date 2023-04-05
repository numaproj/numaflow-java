package io.numaproj.numaflow.function.reduce;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.google.protobuf.ByteString;
import io.numaproj.numaflow.function.HandlerDatum;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.v1.Udfunction;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;

/**
 * Reduce actor invokes the user defined code and returns the result.
 */

@Slf4j
@AllArgsConstructor
public class ReduceActor extends AbstractActor {

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
        Message[] resultMessages = this.groupBy.getOutput(keys, md);
        // send the result back to sender(parent actor)
        getSender().tell(buildDatumListResponse(resultMessages), getSelf());
    }

    private ActorResponse buildDatumListResponse(Message[] messages) {
        Udfunction.DatumList.Builder datumListBuilder = Udfunction.DatumList.newBuilder();
        Arrays.stream(messages).forEach(message -> {
            datumListBuilder.addElements(Udfunction.Datum.newBuilder()
                    .addAllKeys(List.of(message.getKeys()))
                    .setValue(ByteString.copyFrom(message.getValue()))
                    .build());
        });
        return new ActorResponse(this.keys, datumListBuilder.build());
    }

}

