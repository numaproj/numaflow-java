package io.numaproj.numaflow.sessionreducer;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;
import io.numaproj.numaflow.sessionreducer.model.OutputStreamObserver;
import io.numaproj.numaflow.sessionreducer.model.SessionReducer;

/**
 * Session reducer actor invokes user defined functions to handle session reduce requests.
 * Session reducer actor and session window has a one-to-one relationship, meaning
 * a session reducer actor only works on its assigned single session window.
 */
class SessionReducerActor extends AbstractActor {
    // the session window the actor is working on
    private Sessionreduce.KeyedWindow keyedWindow;
    private final SessionReducer sessionReducer;
    private OutputStreamObserver outputStream;
    // when set to true, it means this session is already closed.
    private boolean isClosed = false;

    public SessionReducerActor(
            Sessionreduce.KeyedWindow keyedWindow,
            SessionReducer sessionReducer,
            OutputStreamObserver outputStream) {
        this.keyedWindow = keyedWindow;
        this.sessionReducer = sessionReducer;
        this.outputStream = outputStream;
    }

    public static Props props(
            Sessionreduce.KeyedWindow keyedWindow,
            SessionReducer groupBy,
            ActorRef outputActor) {
        return Props.create(
                SessionReducerActor.class,
                keyedWindow,
                groupBy,
                new OutputStreamObserverImpl(outputActor, keyedWindow)
        );
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
        // update the keyed window
        this.keyedWindow = newKeyedWindow;
        // update the output stream to use the new keyed window
        OutputStreamObserverImpl newOutputStream = (OutputStreamObserverImpl) this.outputStream;
        newOutputStream.setKeyedWindow(newKeyedWindow);
    }

    // when receiving a message, process it.
    // this is for OPEN/APPEND operation.
    private void invokeHandler(HandlerDatum handlerDatum) {
        this.sessionReducer.processMessage(
                keyedWindow.getKeysList().toArray(new String[0]),
                handlerDatum,
                outputStream);
    }

    // receiving an EOF signal, close the window.
    // this is for CLOSE operation or for the close of gRPC input stream.
    private void handleEOF(String EOF) {
        if (this.isClosed) {
            return;
        }
        // invoke handleEndOfStream to materialize the messages received so far.
        this.sessionReducer.handleEndOfStream(
                keyedWindow.getKeysList().toArray(new String[0]),
                outputStream);
        // construct an actor response and send it back to the supervisor actor, indicating the actor
        // has finished processing all the messages for the corresponding keyed window.
        getSender().tell(buildEOFResponse(), getSelf());
        this.isClosed = true;
    }

    // receiving a GetAccumulatorRequest, return the accumulator of the window.
    // this is for MERGE operation.
    private void handleGetAccumulatorRequest(GetAccumulatorRequest getAccumulatorRequest) {
        getSender().tell(buildMergeResponse(
                        this.sessionReducer.accumulator(),
                        getAccumulatorRequest.getMergeTaskId())
                ,
                getSelf());
        // after finishing handling a GetAccumulatorRequest, the session is considered closed.
        this.isClosed = true;
    }

    // receiving a MergeAccumulatorRequest, merge the accumulator.
    // this is for MERGE operation.
    private void handleMergeAccumulatorRequest(MergeAccumulatorRequest mergeAccumulatorRequest) {
        this.sessionReducer.mergeAccumulator(mergeAccumulatorRequest.getAccumulator());
    }

    private ActorResponse buildEOFResponse() {
        Sessionreduce.SessionReduceResponse.Builder responseBuilder = Sessionreduce.SessionReduceResponse.newBuilder();
        responseBuilder.setKeyedWindow(this.keyedWindow);
        responseBuilder.setEOF(true);
        return ActorResponse.builder()
                .response(responseBuilder.build())
                .build();
    }

    private ActorResponse buildMergeResponse(byte[] accumulator, String mergeTaskId) {
        Sessionreduce.SessionReduceResponse.Builder responseBuilder = Sessionreduce.SessionReduceResponse.newBuilder();
        responseBuilder.setKeyedWindow(this.keyedWindow);
        return ActorResponse.builder()
                .response(responseBuilder.build())
                .accumulator(accumulator)
                .mergeTaskId(mergeTaskId)
                .build();
    }
}
