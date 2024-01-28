package io.numaproj.numaflow.sessionreducer;

import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * ActorRequest is used by the supervisor actor to distribute session reduce operations to
 * individual session reducer actors. One actor request is sent to only one session reducer actor.
 */
@Getter
@AllArgsConstructor
class ActorRequest {
    ActorRequestType type;
    // the window of the target session the actor request is sent to
    Sessionreduce.KeyedWindow keyedWindow;
    // the new keyed window the target session is to be expanded to
    // it is specified only when the actor request is an EXPAND
    // TODO - use builder pattern to ensure this is only set when type == EXPAND
    Sessionreduce.KeyedWindow newKeyedWindow;
    // the payload of the request
    Sessionreduce.SessionReduceRequest.Payload payload;
    // the id of the merge task this request belongs to
    // it is specified only when the actor request is a GET_ACCUMULATOR
    // TODO - use builder pattern to ensure this is only set when type == GET_ACCUMULATOR
    String mergeTaskId;
}
