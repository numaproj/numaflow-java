package io.numaproj.numaflow.sink;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import io.numaproj.numaflow.sink.handler.SinkHandler;
import io.numaproj.numaflow.sink.types.ResponseList;
import lombok.extern.slf4j.Slf4j;

/**
 * Sink actor invokes the user defined sink and returns the response back on EOF.
 */

@Slf4j
class SinkActor extends AbstractActor {

    private final SinkHandler sinkHandler;
    private final ResponseList.ResponseListBuilder responseListBuilder = ResponseList.newBuilder();

    public static Props props(SinkHandler sinkHandler) {
        return Props.create(SinkActor.class, sinkHandler);
    }

    public SinkActor(SinkHandler sinkHandler) {
        this.sinkHandler = sinkHandler;
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
        responseListBuilder.addResponse(this.sinkHandler.processMessage(handlerDatum));
    }

    private void getResult(String eof) {
        getContext().getParent().tell(responseListBuilder.build(), ActorRef.noSender());
    }
}

