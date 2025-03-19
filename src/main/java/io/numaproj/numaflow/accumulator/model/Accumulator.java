package io.numaproj.numaflow.accumulator.model;

/**
 * Accumulator exposes methods for performing accumulation operations on the
 * stream of data.
 */
public abstract class Accumulator {
    /**
     * method which will be used for processing messages, gets invoked for every
     * message in the keyed accumulator stream.
     *
     * @param datum current message to be processed by the accumulator
     * @param outputStream observer for sending the output {@link Message} to the
     *         output
     *         stream.
     */
    public abstract void processMessage(
            Datum datum,
            OutputStreamObserver outputStream);

    /**
     * handleEndOfStream handles the closure of the keyed accumulator stream.
     * This method is invoked when the input accumulator stream is closed.
     * It provides the capability of constructing final responses based on the
     * messages processed so far.
     *
     * @param outputStreamObserver observer for sending the output {@link Message}
     *         to the output stream.
     */
    public abstract void handleEndOfStream(
            OutputStreamObserver outputStreamObserver);
}
