package io.numaproj.numaflow.function.reduce;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.HandlerDatum;

/**
 * ReduceDatumStream is an interface which will be passed to reduce handlers to read input
 * messages.
 */
public interface ReduceDatumStream {
    // EOF indicates the end of input
    HandlerDatum EOF = HandlerDatum.EOF();

    /**
     * Reads message from the stream.
     *
     * @return the message read from the stream. null if there are no more messages to consume.
     */
    Datum ReadMessage();

    /**
     * Writes message to the stream.
     *
     * @throws InterruptedException if writing gets interrupted.
     */
    void WriteMessage(Datum datum) throws InterruptedException;
}
