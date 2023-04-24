package io.numaproj.numaflow.sink;

/**
 * SinkHandler exposes method for publishing messages to sink.
 * Implementations should override the processMessage method
 * which will be used for processing the input messages
 */

public abstract class SinkHandler {
    // Function to process a list of coming messages
    public abstract ResponseList processMessage(SinkDatumStream datumStream);
}
