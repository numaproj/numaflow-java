package io.numaproj.numaflow.reducestreamer;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.google.protobuf.Timestamp;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import io.numaproj.numaflow.reducestreamer.model.Metadata;
import io.numaproj.numaflow.reducestreamer.model.OutputStreamObserver;
import io.numaproj.numaflow.reducestreamer.model.ReduceStreamer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Reduce streamer actor invokes user defined functions to handle reduce requests.
 * When receiving an input request, it invokes the processMessage to handle the datum.
 * When receiving an EOF signal from the supervisor, it invokes the handleEndOfStream to execute
 * the user-defined end of stream processing logics.
 */
@Slf4j
@AllArgsConstructor
class ReduceStreamerActor extends AbstractActor {
    private String[] keys;
    private Metadata md;
    private ReduceStreamer groupBy;
    private OutputStreamObserver outputStream;

    public static Props props(
            String[] keys, Metadata md, ReduceStreamer groupBy, ActorRef responseStreamActor) {
        return Props.create(
                ReduceStreamerActor.class,
                keys,
                md,
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
        this.groupBy.processMessage(keys, handlerDatum, outputStream, md);
    }

    private void sendEOF(String EOF) {
        // constructing final responses based on the messages processed so far and sending them out.
        this.groupBy.handleEndOfStream(keys, outputStream, md);
        // constructing an EOF response and sending it back to the supervisor actor.
        getSender().tell(buildEOFResponse(), getSelf());
    }

    private ActorResponse buildEOFResponse() {
        ReduceOuterClass.ReduceResponse.Builder responseBuilder = ReduceOuterClass.ReduceResponse.newBuilder();
        responseBuilder.setWindow(ReduceOuterClass.Window.newBuilder()
                .setStart(Timestamp.newBuilder()
                        .setSeconds(this.md.getIntervalWindow().getStartTime().getEpochSecond())
                        .setNanos(this.md.getIntervalWindow().getStartTime().getNano()))
                .setEnd(Timestamp.newBuilder()
                        .setSeconds(this.md.getIntervalWindow().getEndTime().getEpochSecond())
                        .setNanos(this.md.getIntervalWindow().getEndTime().getNano()))
                .setSlot("slot-0").build());
        responseBuilder.setEOF(true);
        // set a dummy result with the keys.
        responseBuilder.setResult(ReduceOuterClass.ReduceResponse.Result
                .newBuilder()
                .addAllKeys(List.of(this.keys))
                .build());
        return new ActorResponse(responseBuilder.build(), ActorResponseType.EOF_RESPONSE);
    }
}
