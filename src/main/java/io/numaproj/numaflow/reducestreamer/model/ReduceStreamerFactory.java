package io.numaproj.numaflow.reducestreamer.model;

/**
 * ReduceStreamerFactory is the factory for ReduceStreamer.
 */
public abstract class ReduceStreamerFactory<ReduceStreamerT extends ReduceStreamer> {
    /**
     * Helper function to create a reduce streamer.
     *
     * @return a concrete reduce streamer instance
     */
    public abstract ReduceStreamerT createReduceStreamer();
}
