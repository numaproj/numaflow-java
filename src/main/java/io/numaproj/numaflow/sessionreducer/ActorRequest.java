package io.numaproj.numaflow.sessionreducer;

import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;
import lombok.Builder;
import lombok.Getter;

/**
 * ActorRequest is used by the supervisor actor to distribute session reduce operations to
 * individual session reducer actors. One actor request is sent to only one session reducer actor.
 */
@Getter
class ActorRequest {
    private final ActorRequestType type;
    // the window of the target session the actor request is sent to
    private final Sessionreduce.KeyedWindow keyedWindow;
    // the new keyed window the target session is to be expanded to
    // it is specified only when the actor request is an EXPAND
    private final Sessionreduce.KeyedWindow newKeyedWindow;
    // the payload of the request
    private final Sessionreduce.SessionReduceRequest.Payload payload;
    // the id of the merge task this request belongs to, it's equal to the unique id of the merged window,
    // it is specified only when the actor request is a GET_ACCUMULATOR
    private final String mergeTaskId;

    @Builder
    private ActorRequest(
            ActorRequestType type,
            Sessionreduce.KeyedWindow keyedWindow,
            Sessionreduce.KeyedWindow newKeyedWindow,
            Sessionreduce.SessionReduceRequest.Payload payload,
            String mergeTaskId
    ) {
        this.type = type;
        this.keyedWindow = keyedWindow;
        this.newKeyedWindow = newKeyedWindow;
        this.payload = payload;
        this.mergeTaskId = mergeTaskId;
    }

    static class ActorRequestBuilder {
        ActorRequest build() {
            if (newKeyedWindow != null && type != ActorRequestType.EXPAND) {
                throw new IllegalStateException(
                        "attribute newKeyedWindow can only be set when the request is an EXPAND.");
            }
            if (newKeyedWindow == null && type == ActorRequestType.EXPAND) {
                throw new IllegalStateException(
                        "attribute newKeyedWindow must be set when the request is an EXPAND.");
            }
            if (mergeTaskId != null && type != ActorRequestType.GET_ACCUMULATOR) {
                throw new IllegalStateException(
                        "attribute mergeTaskId can only be set when the request is a GET_ACCUMULATOR.");
            }
            if (mergeTaskId == null && type == ActorRequestType.GET_ACCUMULATOR) {
                throw new IllegalStateException(
                        "attribute mergeTaskId must be set when the request is a GET_ACCUMULATOR.");
            }
            return new ActorRequest(type, keyedWindow, newKeyedWindow, payload, mergeTaskId);
        }
    }
}
