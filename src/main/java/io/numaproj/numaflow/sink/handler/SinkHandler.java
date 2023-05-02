package io.numaproj.numaflow.sink.handler;

import io.numaproj.numaflow.sink.interfaces.Datum;
import io.numaproj.numaflow.sink.types.Response;

/**
 * SinkHandler exposes method for publishing messages to sink.
 * Implementations should override the processMessage method
 * which will be used for processing the input messages
 */

public abstract class SinkHandler {
    /**
     * method will be used for processing messages.
     * @param datum current message to be processed
     * @return Response to indicate success or failure.
     */
    public abstract Response processMessage(Datum datum);
}
