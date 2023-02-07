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
                .build();
    }

    public void invokeHandler(HandlerDatum handlerDatum) {
        if (handlerDatum == ReduceDatumStream.EOF) {
            getSender().tell(this.groupBy.getResult(), getSender());
            System.out.println("wrote the output successfully");
            return;
        }
        this.groupBy.readMessage(handlerDatum);
    }

}

