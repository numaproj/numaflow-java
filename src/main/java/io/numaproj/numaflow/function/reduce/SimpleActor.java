package io.numaproj.numaflow.function.reduce;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import io.numaproj.numaflow.function.HandlerDatum;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.metadata.Metadata;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;


@AllArgsConstructor
@NoArgsConstructor
public class SimpleActor extends AbstractActor {

    private String key;
    private Metadata metadata;
    private int result;

    public static Props props(String key, Metadata metadata) {
        return Props.create(SimpleActor.class, key, metadata, 0);
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
            getSender().tell(new Message[]{Message.toAll(String.valueOf(result).getBytes())}, getSender());
            System.out.println("wrote the output successfully");
            return;
        }
        this.result += 1;
    }

}

