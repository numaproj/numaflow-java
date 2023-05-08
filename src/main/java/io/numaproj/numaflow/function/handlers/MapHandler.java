package io.numaproj.numaflow.function.handlers;

import io.numaproj.numaflow.function.interfaces.Datum;
import io.numaproj.numaflow.function.types.MessageList;

/**
 * MapHandler exposes method for performing map operation.
 * Implementations should override the processMessage method
 * which will be used for processing the input messages
 */

public abstract class MapHandler {
    /**
     * method which will be used for processing messages.
     *
     * @param keys message keys
     * @param datum current message to be processed
     * @return MessageList which contains output from map
     */
    public abstract MessageList processMessage(String[] keys, Datum datum);
}
