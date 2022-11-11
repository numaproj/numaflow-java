package io.numaproj.numaflow.function.reduce;

import io.numaproj.numaflow.function.v1.Udfunction;

/**
 * ReduceDatumStream is an interface which will be passed to
 * reduce handlers to read input messages.
 */
public interface ReduceDatumStream {
    /* ReadMessage can be used to read message from the stream
    * returns null if there are no more messages to consume.*/
    Udfunction.Datum ReadMessage();
}
