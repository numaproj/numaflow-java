package io.numaproj.numaflow.sessionreducer;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * MergeAccumulatorRequest is sent from the supervisor actor to the merged session reducer actor to
 * ask the actor to merge an accumulator.
 * <p>
 * "Hey, I received an accumulator from one of the sessions that are merging to you, please merge it with yourself."
 * "Also, you may be interested to know that this one is the last one to merge,
 * so that after merging it, you can mark yourself as no longer in a merging process."
 */
@AllArgsConstructor
@Getter
class MergeAccumulatorRequest {
    private final boolean isLast;
    private final byte[] accumulator;
}
