package io.numaproj.numaflow.function.reduce;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import io.numaproj.numaflow.function.HandlerDatum;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;


@AllArgsConstructor
@NoArgsConstructor
public class ReduceActor extends AbstractActor {

    private GroupBy groupBy;

    public static Props props(GroupBy groupBy) {
        return Props.create(ReduceActor.class, groupBy);
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
        this.groupBy.readMessage(handlerDatum);
    }

    private void getResult(String eof) {
        getSender().tell(this.groupBy.getResult(), getSelf());
    }

}

