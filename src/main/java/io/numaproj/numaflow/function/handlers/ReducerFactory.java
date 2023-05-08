package io.numaproj.numaflow.function.handlers;

/**
 * ReducerFactory is the factory for Reduce handlers.
 */

public abstract class ReducerFactory<ReducerT extends ReduceHandler> {
    public abstract ReducerT createReducer();
}
