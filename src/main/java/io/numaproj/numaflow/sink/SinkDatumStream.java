package io.numaproj.numaflow.sink;

/**
 * SinkDatumStream is an interface which will be passed to
 * sink handlers to read input messages.
 */
public interface SinkDatumStream {
    // EOF indicates the end of input
    HandlerDatum EOF = HandlerDatum.EOF();

    /* ReadMessage can be used to read message from the stream
     * returns null if there are no more messages to consume.*/
    HandlerDatum ReadMessage();
}
