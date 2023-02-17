package io.numaproj.numaflow.function.reduce;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import io.numaproj.numaflow.function.HandlerDatum;
import io.numaproj.numaflow.function.metadata.Metadata;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Reduce actor invokes the user defined code and returns the result.
 */

@Slf4j
@AllArgsConstructor
@NoArgsConstructor
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
        getSender().tell(this.groupBy.getOutput(key, md), getSelf());
    }

}

