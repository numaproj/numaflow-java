package io.numaproj.numaflow.function.reduce;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.metadata.Metadata;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;


@AllArgsConstructor
@NoArgsConstructor
public class SimpleActor extends AbstractActor {

    private ReduceHandler reduceHandler;
    private String key;
    private Metadata metadata;

    public static Props props(String key, Metadata metadata, ReduceHandler reduceHandler) {
        return Props.create(SimpleActor.class, reduceHandler, key, metadata);
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder
                .create()
                .match(ReduceDatumStreamImpl.class, this::invokeHandler)
                .build();
    }

    public void invokeHandler(ReduceDatumStreamImpl reduceDatumStream) {
        Message[] result = this.reduceHandler.HandleDo(this.key, reduceDatumStream, metadata);
        getSender().tell(result, getSender());
    }

}

