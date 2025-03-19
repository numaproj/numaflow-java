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

    public static Props props(
            Accumulator accumulator, ActorRef outputActor) {
        return Props.create(
                AccumulatorActor.class,
                accumulator,
                new OutputStreamObserverImpl(outputActor), outputActor);
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
        System.out.println("Got request");
        this.accumulator.processMessage(handlerDatum, outputStream);
    }

    private void sendEOF(String EOF) {
        // invoke handleEndOfStream to materialize the messages received so far.
        this.accumulator.handleEndOfStream(outputStream);
        // construct an actor response and send it back to the supervisor actor, indicating the actor
        // has finished processing all the messages for the corresponding key set.
        AccumulatorOuterClass.AccumulatorResponse eofResponse = AccumulatorOuterClass.AccumulatorResponse
                .newBuilder()
                .setEOF(true)
                .build();

        outputActor.tell(eofResponse, getSelf());
    }
}
