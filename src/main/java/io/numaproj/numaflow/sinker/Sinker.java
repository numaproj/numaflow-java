package io.numaproj.numaflow.sinker;

/**
 * Sinker exposes method for publishing messages to sink.
 * Implementations should override the processMessage method
 * which will be used for processing the input messages
 */

public abstract class Sinker {
    /**
     * method will be used for processing messages.
     * @param datum current message to be processed
     */
    public abstract void processMessage(Datum datum);

    /**
     * method will be used for returning the responses.
     * each response should contain the id of the message
     * @return ResponseList which contains the responses
     */
    public abstract ResponseList getResponse();
}
