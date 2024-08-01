package io.numaproj.numaflow.batchmapper;


/**
 * An iterator over a collection of {@link Datum} elements.
 * Passed to {@link BatchMapper#processMessage(DatumIterator)} (DatumIterator)} method.
 */
public interface DatumIterator {

    /**
     * Returns the next element in the iterator
     * This method blocks until an element becomes available in the queue.
     * When EOF_DATUM is received, this method will return null and the iterator will be closed.
     *
     * @return the next element in the iterator, null if EOF_DATUM is received or the iterator is already closed
     *
     * @throws InterruptedException if the thread is interrupted while waiting for the next element
     */
    Datum next() throws InterruptedException;
}
