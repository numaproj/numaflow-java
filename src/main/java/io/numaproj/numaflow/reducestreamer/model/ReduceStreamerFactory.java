package io.numaproj.numaflow.reducestreamer.model;

/**
 * ReduceStreamerFactory is the factory for ReduceStreamer.
 */
public abstract class ReduceStreamerFactory<ReduceStreamerT extends ReduceStreamer> {
    public abstract ReduceStreamerT createReduceStreamer();
}
