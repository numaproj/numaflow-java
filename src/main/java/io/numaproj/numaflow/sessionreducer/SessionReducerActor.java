package io.numaproj.numaflow.sessionreducer;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;
import io.numaproj.numaflow.sessionreducer.model.OutputStreamObserver;
import io.numaproj.numaflow.sessionreducer.model.SessionReducer;
import lombok.AllArgsConstructor;

/**
 * TODO - update it
 * Reduce streamer actor invokes user defined functions to handle reduce requests.
 * When receiving an input request, it invokes the processMessage to handle the datum.
 * When receiving an EOF signal from the supervisor, it invokes the handleEndOfStream to execute
 * the user-defined end of stream processing logics.
 */
@AllArgsConstructor
class SessionReducerActor extends AbstractActor {

    private Sessionreduce.KeyedWindow keyedWindow;
    private SessionReducer groupBy;
    private OutputStreamObserver outputStream;

    public static Props props(
            Sessionreduce.KeyedWindow keyedWindow,
            SessionReducer groupBy,
            ActorRef responseStreamActor) {
        return Props.create(
                SessionReducerActor.class,
                keyedWindow,
                groupBy,
                new OutputStreamObserverImpl(responseStreamActor));
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder
                .create()
                .match(HandlerDatum.class, this::invokeHandler)
                .match(String.class, this::handleEOF)
                .build();
    }

    private void invokeHandler(HandlerDatum handlerDatum) {
        this.groupBy.processMessage(
                keyedWindow.getKeysList().toArray(new String[0]),
                handlerDatum,
                outputStream);
    }

    private void handleEOF(String EOF) {
        // invoke handleEndOfStream to materialize the messages received so far.
        this.groupBy.handleEndOfStream(
                keyedWindow.getKeysList().toArray(new String[0]),
                outputStream);
        // construct an actor response and send it back to the supervisor actor, indicating the actor
        // has finished processing all the messages for the corresponding keyed window.
        getSender().tell(buildEOFResponse(), getSelf());
    }

    private ActorResponse buildEOFResponse() {
        Sessionreduce.SessionReduceResponse.Builder responseBuilder = Sessionreduce.SessionReduceResponse.newBuilder();
        responseBuilder.setKeyedWindow(this.keyedWindow);
        responseBuilder.setEOF(true);
        return new ActorResponse(
                responseBuilder.build(),
                false);
    }
}
