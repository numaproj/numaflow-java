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

/**
 * Reduce actor invokes the user defined code and returns the result.
 */

@Slf4j
@AllArgsConstructor
public class ReduceActor extends AbstractActor {

    private String key;
    private Metadata md;
    private Reducer groupBy;

    public static Props props(String key, Metadata md, Reducer groupBy) {
        return Props.create(ReduceActor.class, key, md, groupBy);
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
        this.groupBy.addMessage(key, handlerDatum, md);
    }

    private void getResult(String eof) {
        Message[] resultMessages = this.groupBy.getOutput(key, md);
        // send the result back to sender(parent actor)
        getSender().tell(buildDatumListResponse(resultMessages), getSelf());
    }

    private ActorResponse buildDatumListResponse(Message[] messages) {
        Udfunction.DatumList.Builder datumListBuilder = Udfunction.DatumList.newBuilder();
        Arrays.stream(messages).forEach(message -> {
            datumListBuilder.addElements(Udfunction.Datum.newBuilder()
                    .setKey(message.getKey())
                    .setValue(ByteString.copyFrom(message.getValue()))
                    .build());
        });
        return new ActorResponse(this.key, datumListBuilder.build());
    }

}

