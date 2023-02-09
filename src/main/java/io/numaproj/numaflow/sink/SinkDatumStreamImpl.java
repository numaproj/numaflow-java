package io.numaproj.numaflow.sink;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Logger;

/**
 * Implementation of SinkDatumStream, exposes two methods read and write, it is an unbounded queue,
 * which blocks the reads if the queue is empty and blocks the writes if the queue is full
 */

@Slf4j
public class SinkDatumStreamImpl implements SinkDatumStream {
    private final BlockingQueue<HandlerDatum> blockingQueue = new LinkedBlockingDeque<>();

    // blocking call, returns EOF if there are no messages to be read
    @Override
    public HandlerDatum ReadMessage() {
        HandlerDatum readMessage = null;
        try {
            readMessage = blockingQueue.take();
        } catch (InterruptedException e) {
            log.error(
                    "Error occurred while reading message from datum stream" + e.getMessage());
            Thread.interrupted();
        }
        return readMessage;
    }

    // blocking call, waits until the write operation is successful
    @Override
    public void WriteMessage(HandlerDatum datum) throws InterruptedException {
        blockingQueue.put(datum);
    }
}
