package io.numaproj.numaflow.reducestreamer;


/**
 * ReducerFactory is the factory for Reducer.
 */

public abstract class ReducerFactory<ReducerT extends Reducer> {
    public abstract ReducerT createReducer();
}
