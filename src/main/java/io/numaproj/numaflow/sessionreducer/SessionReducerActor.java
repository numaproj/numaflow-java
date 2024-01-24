package io.numaproj.numaflow.sessionreducer;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import io.numaproj.numaflow.sessionreducer.model.OutputStreamObserver;
import io.numaproj.numaflow.sessionreducer.model.SessionReducer;
import lombok.AllArgsConstructor;

import java.util.List;

/**
 * Reduce streamer actor invokes user defined functions to handle reduce requests.
 * When receiving an input request, it invokes the processMessage to handle the datum.
 * When receiving an EOF signal from the supervisor, it invokes the handleEndOfStream to execute
 * the user-defined end of stream processing logics.
 */
@AllArgsConstructor
class SessionReducerActor extends AbstractActor {
    private String[] keys;
    private SessionReducer groupBy;
    private OutputStreamObserver outputStream;

    public static Props props(
            String[] keys, SessionReducer groupBy, ActorRef responseStreamActor) {
        return Props.create(
                SessionReducerActor.class,
                keys,
                groupBy,
                new OutputStreamObserverImpl(responseStreamActor));
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder
                .create()
                .match(HandlerDatum.class, this::invokeHandler)
                .match(String.class, this::sendEOF)
                .build();
    }

    private void invokeHandler(HandlerDatum handlerDatum) {
        this.groupBy.processMessage(keys, handlerDatum, outputStream);
    }

    private void sendEOF(String EOF) {
        // invoke handleEndOfStream to materialize the messages received so far.
        this.groupBy.handleEndOfStream(keys, outputStream);
        // construct an actor response and send it back to the supervisor actor, indicating the actor
        // has finished processing all the messages for the corresponding key set.
        getSender().tell(buildEOFResponse(), getSelf());
    }

    private ActorResponse buildEOFResponse() {
        ReduceOuterClass.ReduceResponse.Builder responseBuilder = ReduceOuterClass.ReduceResponse.newBuilder();
        /*
        responseBuilder.setWindow(ReduceOuterClass.Window.newBuilder()
                .setStart(Timestamp.newBuilder()
                        .setSeconds(this.md.getIntervalWindow().getStartTime().getEpochSecond())
                        .setNanos(this.md.getIntervalWindow().getStartTime().getNano()))
                .setEnd(Timestamp.newBuilder()
                        .setSeconds(this.md.getIntervalWindow().getEndTime().getEpochSecond())
                        .setNanos(this.md.getIntervalWindow().getEndTime().getNano()))
                .setSlot("slot-0").build());
         */
        responseBuilder.setEOF(true);
        // set a dummy result with the keys.
        responseBuilder.setResult(ReduceOuterClass.ReduceResponse.Result
                .newBuilder()
                .addAllKeys(List.of(this.keys))
                .build());
        return new ActorResponse(
                responseBuilder.build(),
                false);
    }
}
