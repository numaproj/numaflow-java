package io.numaproj.numaflow.sinker;

/**
 * Sinker exposes method for publishing messages to sink.
 * Implementations should override the processMessage method
 * which will be used for processing the input messages
 */

public abstract class Sinker {
    /**
     * method will be used for processing messages.
     * response for the message should be added to the
     * response list using ResponseListBuilder and the
     * response list should be returned.
     *
     * @param datumStream stream of messages to be processed
     * @return response list
     */
    public abstract ResponseList processMessages(DatumIterator datumStream);
}
