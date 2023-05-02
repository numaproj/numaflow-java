package io.numaproj.numaflow.function.handlers;

/**
 * ReducerFactory is used to create Reducer object.
 */

public abstract class ReducerFactory<ReducerT extends ReduceHandler> {
    public abstract ReducerT createReducer();
}
