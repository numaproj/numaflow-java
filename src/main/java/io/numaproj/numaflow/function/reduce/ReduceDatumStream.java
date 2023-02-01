package io.numaproj.numaflow.function.reduce;

import io.numaproj.numaflow.function.v1.Udfunction;

/**
 * ReduceDatumStream is an interface which will be passed to reduce handlers to read input
 * messages.
 */
public interface ReduceDatumStream {

    // EOF indicates the end of input
    Udfunction.Datum EOF = Udfunction.Datum.newBuilder().setKey("EOF").build();

    /**
     * Reads message from the stream.
     *
     * @return the message read from the stream. null if there are no more messages to consume.
     */
    Udfunction.Datum ReadMessage();

    /**
     * Writes message to the stream.
     *
     * @throws InterruptedException if writing gets interrupted.
     */
    void WriteMessage(Udfunction.Datum datum) throws InterruptedException;
}
