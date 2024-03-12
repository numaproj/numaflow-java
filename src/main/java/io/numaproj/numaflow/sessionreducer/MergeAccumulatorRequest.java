package io.numaproj.numaflow.sessionreducer;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * MergeAccumulatorRequest is sent from the supervisor actor to the merged session reducer actor to
 * ask the actor to merge an accumulator.
 * <p>
 * "Hey, I received an accumulator from one of the sessions that are merging to you, please merge it with yourself."
 */
@AllArgsConstructor
@Getter
class MergeAccumulatorRequest {
    private final byte[] accumulator;
}
