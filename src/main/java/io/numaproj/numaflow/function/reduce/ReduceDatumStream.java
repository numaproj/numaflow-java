package io.numaproj.numaflow.function.reduce;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.HandlerDatum;

/**
 * ReduceDatumStream is an interface which will be passed to
 * reduce handlers to read input messages.
 */
public interface ReduceDatumStream {
    // EOF indicates the end of input
    HandlerDatum EOF = new HandlerDatum();

    /* ReadMessage can be used to read message from the stream
     * returns null if there are no more messages to consume.*/
    Datum ReadMessage();
}
