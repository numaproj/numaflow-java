package io.numaproj.numaflow.reducestreamer;


/**
 * Reducer exposes methods for performing reduce operation.
 */


public abstract class Reducer {
    /**
     * addMessage will be invoked for each input message.
     * It can be used to read the input data from datum and
     * update the result for given keys.
     *
     * @param keys message keys
     * @param datum current message to be processed
     * @param md metadata which contains window information
     */
    public abstract void addMessage(String[] keys, Datum datum, Metadata md);

    /**
     * getOutput will be invoked at the end of input.
     *
     * @param keys message keys
     * @param md metadata which contains window information
     *
     * @return MessageList output value, aggregated result
     */
    public abstract MessageList getOutput(String[] keys, Metadata md);
}
