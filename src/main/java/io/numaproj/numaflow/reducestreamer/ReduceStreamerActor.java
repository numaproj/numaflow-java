package io.numaproj.numaflow.reducestreamer;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import io.numaproj.numaflow.reducestreamer.model.HandlerDatum;
import io.numaproj.numaflow.reducestreamer.model.Metadata;
import io.numaproj.numaflow.reducestreamer.user.OutputStreamObserver;
import io.numaproj.numaflow.reducestreamer.user.OutputStreamObserverImpl;
import io.numaproj.numaflow.reducestreamer.user.ReduceStreamer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Reduce stream actor invokes user defined functions to handle reduce request.
 * When receiving an input request, it invokes the processMessage to handle the datum.
 * When receiving an EOF signal from the supervisor, it invokes the handleEndOfStream to execute
 * the user-defined end of stream processing logics.
 */
@Slf4j
@AllArgsConstructor
public class ReduceStreamerActor extends AbstractActor {
    private String[] keys;
    private Metadata md;
    private ReduceStreamer groupBy;
    private OutputStreamObserver outputStream;

    public static Props props(
            String[] keys, Metadata md, ReduceStreamer groupBy,
            StreamObserver<ReduceOuterClass.ReduceResponse> responseStreamObserver) {
        return Props.create(
                ReduceStreamerActor.class,
                keys,
                md,
                groupBy,
                new OutputStreamObserverImpl(md, responseStreamObserver));
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
        this.groupBy.handleEndOfStream(keys, outputStream, md);
        getSender().tell(buildEOFResponse(), getSelf());
    }

    private ActorEOFResponse buildEOFResponse() {
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
        return new ActorEOFResponse(responseBuilder.build());
    }
}
