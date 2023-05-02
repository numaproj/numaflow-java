package io.numaproj.numaflow.function.handlers;

import io.numaproj.numaflow.function.interfaces.Datum;
import io.numaproj.numaflow.function.types.MessageTList;

/**
 * MapTHandler exposes method for performing transform operation
 * in the source. Implementations should override the processMessage
 * method which will be used for transforming and assigning event time
 * to input messages
 */

public abstract class MapTHandler {
    /**
     * method which will be used for processing messages.
     *
     * @param keys message keys
     * @param datum current message to be processed
     * @return MessageTList which contains output from mapT
     */
    public abstract MessageTList processMessage(String[] keys, Datum datum);

}
