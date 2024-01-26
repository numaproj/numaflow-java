package io.numaproj.numaflow.sessionreducer;

import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * TODO - Update
 * ActorRequest is used by the supervisor actor to distribute session reduce operations to individual session reducer actors.
 * One actor request is sent to only one session reducer actor.
 */
@Getter
@AllArgsConstructor
class ActorRequest {
    ActorRequestType type;
    Sessionreduce.KeyedWindow keyedWindow;
    // this is specified when the actor request is an EXPAND.
    // TODO - use builder pattern to ensure this is only set when type == EXPAND
    Sessionreduce.KeyedWindow newKeyedWindow;
    Sessionreduce.SessionReduceRequest.Payload payload;
    // this is specified when the actor request is a GET_ACCUMULATOR.
    // TODO - use builder pattern to ensure this is only set when type == GET_ACCUMULATOR
    String mergeWindowId;
}
