package io.numaproj.numaflow.reducestreamer.user;


/**
 * ReducerFactory is the factory for Reducer.
 * <p>
 * TODO - do we need this?
 */

public abstract class ReduceStreamerFactory<ReduceStreamerT extends ReduceStreamer> {
    public abstract ReduceStreamerT createReduceStreamer();
}
