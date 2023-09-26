package io.numaproj.numaflow.sinker;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * An implementation of {@link DatumIterator} that uses a blocking queue to store the elements.
 */
@Slf4j
class DatumIteratorImpl implements DatumIterator {
    private final BlockingQueue<Datum> blockingQueue = new LinkedBlockingDeque<>();
    private Datum nextElement;
    private boolean isClosed = false;

    /*
     * Returns true if there is at least one more element in the iterator, or false otherwise.
     * This method blocks until an element becomes available in the blocking queue.
     * When EOF is received, this method will return false and the iterator will be closed.
     */
    @Override
    public boolean hasNext() {
        if (nextElement != null) {
            return true;
        }
        try {
            nextElement = blockingQueue.take(); // block until an element becomes available
            if (nextElement.equals(HandlerDatum.EOF_DATUM)) {
                isClosed = true;
                return false;
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public Datum next() {
        if (isClosed || !hasNext()) {
            throw new IllegalStateException("No more elements");
        }
        Datum element = nextElement;
        nextElement = null;
        return element;
    }

    // blocking call, waits until the write operation is successful
    public void writeMessage(Datum datum) throws InterruptedException {
        blockingQueue.put(datum);
    }
}
