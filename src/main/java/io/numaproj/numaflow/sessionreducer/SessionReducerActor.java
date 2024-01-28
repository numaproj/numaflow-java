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
 * Session reducer actor invokes user defined functions to handle session reduce requests.
 * Session reducer actor and session window has a one-to-one relationship, meaning
 * a session reducer actor only works on its assigned single session window.
 */
@AllArgsConstructor
class SessionReducerActor extends AbstractActor {
    // the session window the actor is working on
    private Sessionreduce.KeyedWindow keyedWindow;
    private SessionReducer sessionReducer;
    private OutputStreamObserver outputStream;
    // when set to true, it means the corresponding session is in the process of merging with other windows.
    private boolean isMerging;
    // TODO - add a isClosed flag because both EOF and CLOSE operation can trigger EOF and a session should only deal with it once

    public static Props props(
            Sessionreduce.KeyedWindow keyedWindow,
            SessionReducer groupBy,
            ActorRef outputActor,
            boolean isMerging) {
        return Props.create(
                SessionReducerActor.class,
                keyedWindow,
                groupBy,
                new OutputStreamObserverImpl(outputActor, keyedWindow),
                isMerging);
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder
                .create()
                .match(Sessionreduce.KeyedWindow.class, this::updateKeyedWindow)
                .match(HandlerDatum.class, this::invokeHandler)
                .match(String.class, this::handleEOF)
                .match(GetAccumulatorRequest.class, this::handleGetAccumulatorRequest)
                .match(MergeAccumulatorRequest.class, this::handleMergeAccumulatorRequest)
                .build();
    }

    // receiving a new keyed window, update the keyed window.
    // this is for EXPAND operation.
    private void updateKeyedWindow(Sessionreduce.KeyedWindow newKeyedWindow) {
        if (this.isMerging) {
            throw new RuntimeException(
                    "cannot expand a session window when it's in a merging process."
                            + " window info: " + this.keyedWindow.toString());
        }
        // update the keyed window
        this.keyedWindow = newKeyedWindow;
        // update the output stream to use the new keyed window
        OutputStreamObserverImpl newOutputStream = (OutputStreamObserverImpl) this.outputStream;
        newOutputStream.setKeyedWindow(newKeyedWindow);
        this.outputStream = newOutputStream;
    }

    // when receiving a message, process it.
    // this is for OPEN/APPEND operation.
    private void invokeHandler(HandlerDatum handlerDatum) {
        if (this.isMerging) {
            throw new RuntimeException(
                    "cannot process messages when the session window is in a merging process."
                            + " window info: " + this.keyedWindow.toString());
        }
        this.sessionReducer.processMessage(
                keyedWindow.getKeysList().toArray(new String[0]),
                handlerDatum,
                outputStream);
    }

    // receiving an EOF signal, close the window.
    // this is for CLOSE operation and the close of gRPC input stream.
    private void handleEOF(String EOF) {
        // FIXME: if a session receives an EOF, if it's in a merging process, it should finish merging and then close itself, instead of throws.
        if (this.isMerging) {
            throw new RuntimeException(
                    "cannot process EOF when the session window is in a merging process."
                            + " window info: " + this.keyedWindow.toString());
        }
        // invoke handleEndOfStream to materialize the messages received so far.
        this.sessionReducer.handleEndOfStream(
                keyedWindow.getKeysList().toArray(new String[0]),
                outputStream);
        // construct an actor response and send it back to the supervisor actor, indicating the actor
        // has finished processing all the messages for the corresponding keyed window.
        getSender().tell(buildEOFResponse(), getSelf());
    }

    // receiving a GetAccumulatorRequest, return the accumulator of the window.
    // this is for MERGE operation.
    private void handleGetAccumulatorRequest(GetAccumulatorRequest getAccumulatorRequest) {
        if (this.isMerging) {
            throw new RuntimeException(
                    "cannot process a GetAccumulatorRequest when the session window is already in a merging process."
                            + " window info: " + this.keyedWindow.toString());
        }
        this.isMerging = true;
        getSender().tell(
                new GetAccumulatorResponse(
                        this.keyedWindow,
                        getAccumulatorRequest.getMergeTaskId(),
                        this.sessionReducer.accumulator()),
                getSelf());
    }

    // receiving a MergeAccumulatorRequest, merge the accumulator.
    // this is for MERGE operation.
    private void handleMergeAccumulatorRequest(MergeAccumulatorRequest mergeAccumulatorRequest) {
        if (!this.isMerging) {
            throw new RuntimeException(
                    "received a merge accumulator request but the session is not in a merging process.");
        }
        System.out.println("keran-merge-test: I am merging an accumulator...");
        this.sessionReducer.mergeAccumulator(mergeAccumulatorRequest.getAccumulator());
        if (mergeAccumulatorRequest.isLast) {
            // TODO - remove test logs.
            System.out.println(
                    "keran-merge-test: this is the last accumulator, I am marking myself as not in merging any more.");
            // this is the last accumulator to merge, mark the actor as no longer merging.
            this.isMerging = false;
        }
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
