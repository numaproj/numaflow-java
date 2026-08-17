package io.numaproj.numaflow.accumulator;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import io.numaproj.numaflow.accumulator.model.Accumulator;
import io.numaproj.numaflow.accumulator.model.OutputStreamObserver;
import io.numaproj.numaflow.accumulator.v1.AccumulatorOuterClass;
import lombok.AllArgsConstructor;

/**
 * Accumulator Actor is responsible for invoking accumulator handle and handling exceptions.
 */
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
                .match(AccumulatorOuterClass.KeyedWindow.class, this::handleCloseWindow)
                .match(String.class, this::sendEOF)
                .build();
    }

    private void invokeHandler(HandlerDatum handlerDatum) {
        this.accumulator.processMessage(handlerDatum, outputStream);
    }

    // CLOSE: echo the exact close window (including slot)
    private void handleCloseWindow(AccumulatorOuterClass.KeyedWindow closeWindow) {
        sendEOFResponse(closeWindow);
    }

    // Fallback: the input stream completed without a CLOSE (broadcast EOF). Keep prior
    // behavior — echo the OPEN window (start/end/keys).
    private void sendEOF(String EOF) {
        sendEOFResponse(AccumulatorOuterClass.KeyedWindow
                .newBuilder()
                .setStart(this.keyedWindow.getStart())
                .setEnd(this.keyedWindow.getEnd())
                .addAllKeys(this.keyedWindow.getKeysList())
                .build());
    }

    private void sendEOFResponse(AccumulatorOuterClass.KeyedWindow eofWindow) {
        // invoke handleEndOfStream to materialize the messages received so far.
        this.accumulator.handleEndOfStream(outputStream);

        AccumulatorOuterClass.AccumulatorResponse eofResponse = AccumulatorOuterClass.AccumulatorResponse
                .newBuilder()
                .setWindow(eofWindow)
                .setEOF(true)
                .build();

        outputActor.tell(eofResponse, getSelf());
    }
}
