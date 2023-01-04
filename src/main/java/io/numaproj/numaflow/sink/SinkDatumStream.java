package io.numaproj.numaflow.sink;

import io.numaproj.numaflow.sink.v1.Udsink;

/**
 * SinkDatumStream is an interface which will be passed to
 * sink handlers to read input messages.
 */
public interface SinkDatumStream {
    // EOF indicates the end of input
    Udsink.Datum EOF = Udsink.Datum.newBuilder().setKey("EOF").build();

    /* ReadMessage can be used to read message from the stream
     * returns null if there are no more messages to consume.*/
    Udsink.Datum ReadMessage();
}
