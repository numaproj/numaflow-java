package io.numaproj.numaflow.reducer;


/**
 * ReducerFactory is the factory for Reducer.
 */

public abstract class ReducerFactory<ReducerT extends Reducer> {
    public abstract ReducerT createReducer();
}
