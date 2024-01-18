package io.numaproj.numaflow.reducestreamer.user;

import io.numaproj.numaflow.reducestreamer.model.Datum;
import io.numaproj.numaflow.reducestreamer.model.Metadata;

/**
 * ReduceStreamer exposes methods for performing reduce streaming operations.
 */
public abstract class ReduceStreamer {
    /**
     * processMessage is invoked for each reduce input message.
     * It reads the input data from the datum and performs reduce operations for the given keys.
     * An output stream is provided for sending back the result to the reduce output stream.
     *
     * @param keys message keys
     * @param datum current message to be processed
     * @param outputStream observer of the reduce result, which is used to send back reduce responses
     * @param md metadata associated with the window
     */
    public abstract void processMessage(
            String[] keys,
            Datum datum,
            OutputStreamObserver outputStream,
            Metadata md);

    /**
     * handleEndOfStream handles the closure of the reduce input stream.
     * This method is invoked when the input reduce stream is closed.
     * It provides the capability of constructing final responses based on the messages processed so far.
     *
     * @param keys message keys
     * @param outputStreamObserver observer of the reduce result, which is used to send back reduce responses
     * @param md metadata associated with the window
     */
    public abstract void handleEndOfStream(
            String[] keys,
            OutputStreamObserver outputStreamObserver,
            Metadata md);
}
