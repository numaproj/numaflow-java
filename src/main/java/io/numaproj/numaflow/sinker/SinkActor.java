package io.numaproj.numaflow.sinker;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import lombok.extern.slf4j.Slf4j;

/**
 * Sink actor invokes the user defined sink and returns the response back on EOF.
 */

@Slf4j
class SinkActor extends AbstractActor {

    private final Sinker sinker;
    private final ResponseList.ResponseListBuilder responseListBuilder = ResponseList.newBuilder();

    public static Props props(Sinker sinker) {
        return Props.create(SinkActor.class, sinker);
    }

    public SinkActor(Sinker sinker) {
        this.sinker = sinker;
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
        responseListBuilder.addResponse(this.sinker.processMessage(handlerDatum));
    }

    private void getResult(String eof) {
        getContext().getParent().tell(responseListBuilder.build(), ActorRef.noSender());
    }
}

