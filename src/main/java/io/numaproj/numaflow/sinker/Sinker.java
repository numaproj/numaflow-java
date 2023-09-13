package io.numaproj.numaflow.sinker;

/**
 * Sinker exposes method for publishing messages to sink.
 * Implementations should override the processMessage method
 * which will be used for processing the input messages
 */

public abstract class Sinker {
    /**
     * Process a message and return a response.
     *
     * @param datum current message to be processed.
     *
     * @return response indicating whether the message processing is successful or not.
     */
    public abstract Response processMessage(Datum datum);
}
