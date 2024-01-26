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
    // when true, it means the corresponding session is in the process of merging with other windows.
    private boolean isMerging;

    public static Props props(
            Sessionreduce.KeyedWindow keyedWindow,
            SessionReducer groupBy,
            ActorRef responseStreamActor,
            boolean isMerging) {
        return Props.create(
                SessionReducerActor.class,
                keyedWindow,
                groupBy,
                new OutputStreamObserverImpl(responseStreamActor, keyedWindow),
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

    private void updateKeyedWindow(Sessionreduce.KeyedWindow newKeyedWindow) {
        if (this.isMerging) {
            return;
        }
        this.keyedWindow = newKeyedWindow;
        // TODO - can we figure out a better way to update the output stream with the new keyed window?
        OutputStreamObserverImpl newOutputStream = (OutputStreamObserverImpl) this.outputStream;
        newOutputStream.setKeyedWindow(newKeyedWindow);
        this.outputStream = newOutputStream;
    }

    private void invokeHandler(HandlerDatum handlerDatum) {
        if (this.isMerging) {
            return;
        }
        this.groupBy.processMessage(
                keyedWindow.getKeysList().toArray(new String[0]),
                handlerDatum,
                outputStream);
    }

    private void handleEOF(String EOF) {
        if (this.isMerging) {
            System.out.println(
                    "keran-merge-test: I received an EOF, but I am in the middle of merging. I am skipping handling EOF.");
            return;
        }
        // invoke handleEndOfStream to materialize the messages received so far.
        this.groupBy.handleEndOfStream(
                keyedWindow.getKeysList().toArray(new String[0]),
                outputStream);
        // construct an actor response and send it back to the supervisor actor, indicating the actor
        // has finished processing all the messages for the corresponding keyed window.
        getSender().tell(buildEOFResponse(), getSelf());
    }

    private void handleGetAccumulatorRequest(GetAccumulatorRequest getAccumulatorRequest) {
        this.isMerging = true;
        getSender().tell(
                new GetAccumulatorResponse(
                        this.keyedWindow,
                        getAccumulatorRequest.getMergeTaskId(),
                        this.groupBy.accumulator()),
                getSelf());
    }

    private void handleMergeAccumulatorRequest(MergeAccumulatorRequest mergeAccumulatorRequest) {
        if (!this.isMerging) {
            throw new RuntimeException(
                    "received a merge accumulator request but the session is not in a merging process.");
        }
        System.out.println("keran-merge-test: I am merging an accumulator...");
        this.groupBy.mergeAccumulator(mergeAccumulatorRequest.getAccumulator());
        if (mergeAccumulatorRequest.isLast) {
            System.out.println(
                    "keran-merge-test: this is the last accumulator, I am marking myself as not in merging any more.");
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
