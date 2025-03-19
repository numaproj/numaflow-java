package io.numaproj.numaflow.accumulator.model;

/**
 * AccumulatorFactory is the factory for Accumulator.
 */
public abstract class AccumulatorFactory<AccumulatorT extends Accumulator> {
    /**
     * Create a concrete instance of Accumulator, will be invoked for
     * every keyed stream. Separate accumulator instances for used for
     * processing keyed streams.
     *
     * @return a concrete instance of Accumulator
     */
    public abstract AccumulatorT createAccumulator();
}
