package io.numaproj.numaflow.sinker;

/**
 * An iterator over a collection of {@link Datum} elements.
 * Passed to {@link Sinker#processMessages(DatumIterator)} method.
 */
public interface DatumIterator {

    /**
     * Returns the next element in the iterator
     * This method blocks until an element becomes available in the blocking queue.
     * When EOF is received, this method will return null and the iterator will be closed.
     *
     * @return the next element in the iterator
     *
     * @throws InterruptedException if the thread is interrupted while waiting for the next element
     */
    Datum next() throws InterruptedException;
}
