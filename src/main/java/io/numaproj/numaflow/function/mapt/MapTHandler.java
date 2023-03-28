package io.numaproj.numaflow.function.mapt;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.MessageT;

/**
 * MapperT exposes method for performing transform operation
 * in the source. Implementations should override the processMessage
 * method which will be used for transforming the input messages
 */

public abstract class MapTHandler {

    /*
        processMessage will be invoked for each input message.
        this method will be used for processing and transforming
        the messages
     */
    public abstract MessageT[] processMessage(String key, Datum datum);

}
