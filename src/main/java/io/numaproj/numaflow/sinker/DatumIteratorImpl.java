package io.numaproj.numaflow.sinker;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A thread-safe implementation of {@link DatumIterator}, backed by a blocking queue.
 */
@Slf4j
class DatumIteratorImpl implements DatumIterator {
    private final BlockingQueue<Datum> blockingQueue = new LinkedBlockingDeque<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Override
    public Datum next() throws InterruptedException {
        // if the iterator is closed, return null
        if (closed.get()) {
            return null;
        }
        Datum datum = blockingQueue.take();
        // if EOF is received, close the iterator and return null
        if (datum == HandlerDatum.EOF_DATUM) {
            closed.set(true);
            return null;
        }
        return datum;
    }

    // blocking call, waits until the write operation is successful
    public void writeMessage(Datum datum) throws InterruptedException {
        blockingQueue.put(datum);
    }
}
