package io.numaproj.numaflow.function.map;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.Message;

/**
 * MapHandler exposes method for performing map operation.
 * Implementations should override the processMessage method
 * which will be used for processing the input messages
 */

public abstract class MapHandler {
    /*
        processMessage will be invoked for each input message.
        this method will be used for processing messages
     */
    public abstract Message[] processMessage(String[] key, Datum datum);
}
