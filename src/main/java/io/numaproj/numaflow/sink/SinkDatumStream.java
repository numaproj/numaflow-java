package io.numaproj.numaflow.sink;

/**
 * SinkDatumStream is an interface which will be passed to sink handlers to read input messages.
 */
public interface SinkDatumStream {
    // EOF indicates the end of input
    HandlerDatum EOF = HandlerDatum.EOF();

    /**
     * Reads message from the stream.
     *
     * @return the message read from the stream. null if there are no more messages to consume.
     */
    HandlerDatum ReadMessage();

    /**
     * Writes message to the stream.
     *
     * @throws InterruptedException if writing gets interrupted.
     */
    void WriteMessage(HandlerDatum datum) throws InterruptedException;
}
