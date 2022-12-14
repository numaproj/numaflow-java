package io.numaproj.numaflow.sink;

import io.numaproj.numaflow.sink.v1.Udsink;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Implementation of SinkDatumStream, exposes two methods
 * read and write, it is an unbounded queue, which blocks
 * the reads if the queue is empty and blocks the writes if
 * the queue is full
 */
public class SinkDatumStreamImpl implements SinkDatumStream {

    public static final Udsink.Datum DONE = Udsink.Datum
            .newBuilder()
            .setKey("DONE")
            .build();
    private final BlockingQueue<Udsink.Datum> blockingQueue = new LinkedBlockingDeque<>();

    // blocking call, returns null if there are no messages to be read
    @Override
    public Udsink.Datum ReadMessage() {
        Udsink.Datum readMessage;
        try {
            readMessage = blockingQueue.take();
            // to indicate close of book to the reader
            if (readMessage == DONE) {
                return null;
            }
        } catch (InterruptedException e) {
            return null;
        }
        return readMessage;
    }

    // blocking call, waits until the write operation is successful
    public void WriteMessage(Udsink.Datum datum) throws InterruptedException {
        blockingQueue.put(datum);
    }
}
