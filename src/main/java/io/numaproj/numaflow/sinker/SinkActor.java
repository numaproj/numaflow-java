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

    // invokeHandler is called by the sink server when a new message is received.
    private void invokeHandler(HandlerDatum handlerDatum) {
        Response response = this.sinker.processMessage(handlerDatum);
        responseListBuilder.addResponse(response);
    }

    // getResult is called by the sink server when EOF is received.
    private void getResult(String eof) {
        ResponseList responseList;
        // Reset the builder after building the response to avoid keeping old responses in memory
        // this is required as the same sinker instance is used for multiple requests
        try {
            responseList = responseListBuilder.build();
        } finally {
            responseListBuilder.clearResponses();
        }
        getContext().getParent().tell(responseList, ActorRef.noSender());
    }
}

