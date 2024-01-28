package io.numaproj.numaflow.sessionreducer;

import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * GetAccumulatorResponse is sent from a session reducer actor back to the supervisor actor,
 * containing the accumulator of the session.
 * <p>
 * "Hey supervisor, I am the session window fromKeyedWindow,
 * I am returning my accumulator so that you can ask mergeTaskId to merge me."
 */
@AllArgsConstructor
@Getter
class GetAccumulatorResponse {
    Sessionreduce.KeyedWindow fromKeyedWindow;
    String mergeTaskId;
    byte[] accumulator;
}
