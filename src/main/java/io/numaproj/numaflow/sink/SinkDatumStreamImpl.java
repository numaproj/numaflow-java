package io.numaproj.numaflow.sink;

import io.numaproj.numaflow.sink.v1.Udsink;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Logger;

/**
 * Implementation of SinkDatumStream, exposes two methods read and write, it is an unbounded queue,
 * which blocks the reads if the queue is empty and blocks the writes if the queue is full
 */
public class SinkDatumStreamImpl implements SinkDatumStream {
    private static final Logger logger = Logger.getLogger(SinkDatumStreamImpl.class.getName());
    private final BlockingQueue<Udsink.Datum> blockingQueue = new LinkedBlockingDeque<>();

    // blocking call, returns EOF if there are no messages to be read
    @Override
    public Udsink.Datum ReadMessage() {
        Udsink.Datum readMessage = null;
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
    @Override
    public void WriteMessage(Udsink.Datum datum) throws InterruptedException {
        blockingQueue.put(datum);
    }
}
