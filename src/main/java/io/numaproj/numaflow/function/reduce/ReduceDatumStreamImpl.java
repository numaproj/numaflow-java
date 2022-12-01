package io.numaproj.numaflow.function.reduce;

import io.numaproj.numaflow.function.v1.Udfunction;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Implementation of ReduceDatumStream, exposes two methods
 * read and write, it is an unbounded queue, which blocks
 * the reads if the queue is empty and blocks the writes if
 * the queue is full
 */
public class ReduceDatumStreamImpl implements ReduceDatumStream {

    public static final Udfunction.Datum DONE = Udfunction.Datum
            .newBuilder()
            .setKey("DONE")
            .build();
    private final BlockingQueue<Udfunction.Datum> blockingQueue = new LinkedBlockingDeque<>();

    // blocking call, returns null if there are no messages to be read
    @Override
    public Udfunction.Datum ReadMessage() {
        Udfunction.Datum readMessage;
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
    public void WriteMessage(Udfunction.Datum datum) throws InterruptedException {
        blockingQueue.put(datum);
    }
}
