package io.numaproj.numaflow.function.reduce;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.MessageList;
import io.numaproj.numaflow.function.metadata.Metadata;

/**
 * ReduceHandler exposes methods for performing reduce operation.
 */


public abstract class ReduceHandler {
    /*
        addMessage will be invoked for each input message.
        It can be used to read the input data from datum and
        update the result for given keys.
     */
    public abstract void addMessage(String[] keys, Datum datum, Metadata md);

    /*
        getOutput will be invoked at the end of input.
        It can is used to return the aggregated result.
     */
    public abstract MessageList getOutput(String[] keys, Metadata md);
}
