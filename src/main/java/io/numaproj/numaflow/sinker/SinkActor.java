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

    public SinkActor(Sinker sinker) {
        this.sinker = sinker;
    }

    public static Props props(Sinker sinker) {
        return Props.create(SinkActor.class, sinker);
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder
                .create()
                .match(HandlerDatum.class, this::invokeHandler)
                .match(String.class, this::getResult)
                .build();
    }

    // invokeHandler is called by the sink server when a new message is received.
    private void invokeHandler(HandlerDatum handlerDatum) {
        this.sinker.processMessage(handlerDatum);
    }

    // getResult is called by the sink server when EOF is received.
    private void getResult(String eof) {
        ResponseList responseList = this.sinker.getResponse();
        getContext().getParent().tell(responseList, ActorRef.noSender());
    }
}

