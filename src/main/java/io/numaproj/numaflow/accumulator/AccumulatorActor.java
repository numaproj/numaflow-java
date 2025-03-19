package io.numaproj.numaflow.accumulator;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import io.numaproj.numaflow.accumulator.model.Accumulator;
import io.numaproj.numaflow.accumulator.model.OutputStreamObserver;
import io.numaproj.numaflow.accumulator.v1.AccumulatorOuterClass;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class AccumulatorActor extends AbstractActor {
    private Accumulator accumulator;
    private OutputStreamObserver outputStream;
    private ActorRef outputActor;
    private AccumulatorOuterClass.KeyedWindow keyedWindow;

    public static Props props(
            Accumulator accumulator,
            ActorRef outputActor,
            AccumulatorOuterClass.KeyedWindow keyedWindow) {
        return Props.create(
                AccumulatorActor.class,
                accumulator,
                new OutputStreamObserverImpl(outputActor, keyedWindow), outputActor, keyedWindow);
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
        this.accumulator.processMessage(handlerDatum, outputStream);
    }

    private void sendEOF(String EOF) {
        // invoke handleEndOfStream to materialize the messages received so far.
        this.accumulator.handleEndOfStream(outputStream);

        AccumulatorOuterClass.AccumulatorResponse eofResponse = AccumulatorOuterClass.AccumulatorResponse
                .newBuilder()
                .setWindow(AccumulatorOuterClass.KeyedWindow
                        .newBuilder()
                        .addAllKeys(this.keyedWindow.getKeysList()))
                .setEOF(true)
                .build();

        outputActor.tell(eofResponse, getSelf());
    }
}
