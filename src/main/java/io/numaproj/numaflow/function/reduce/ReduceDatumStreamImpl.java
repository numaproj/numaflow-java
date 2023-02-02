package io.numaproj.numaflow.function.reduce;

import io.numaproj.numaflow.function.v1.Udfunction;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Logger;

/**
 * Implementation of ReduceDatumStream, exposes two methods read and write, it is an unbounded
 * queue, which blocks the reads if the queue is empty and blocks the writes if the queue is full
 */
public class ReduceDatumStreamImpl implements ReduceDatumStream {
    private static final Logger logger = Logger.getLogger(ReduceDatumStreamImpl.class.getName());
    private final BlockingQueue<Udfunction.Datum> blockingQueue = new LinkedBlockingDeque<>();

    // TODO - unit test this function.
    // blocking call, returns EOF if there are no messages to be read
    @Override
    public Udfunction.Datum ReadMessage() {
        Udfunction.Datum readMessage = null;
        try {
            readMessage = blockingQueue.take();
        } catch (InterruptedException e) {
            logger.severe(
                    "Error occurred while reading message from datum stream" + e.getMessage());
            Thread.interrupted();
        }
        return readMessage;
    }

    // blocking call, waits until the write operation is successful
    public void WriteMessage(Udfunction.Datum datum) throws InterruptedException {
        blockingQueue.put(datum);
    }
}
