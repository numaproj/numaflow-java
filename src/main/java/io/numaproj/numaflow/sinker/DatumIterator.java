package io.numaproj.numaflow.sinker;

/**
 * An iterator over a collection of {@link Datum} elements.
 * Passed to {@link Sinker#processMessages(DatumIterator)} method.
 */
public interface DatumIterator {
    /**
     * Returns true if there is at least one more element in the iterator, or false otherwise.
     * This method blocks until an element becomes available in the blocking queue.
     * This method should be called before calling {@link #next()} to ensure that the iterator is not empty.
     * otherwise {@link #next()} will throw an {@link IllegalStateException}.
     *
     * @return true if there is at least one more element in the iterator, or false otherwise
     */
    boolean hasNext();

    /**
     * Returns the next element in the iterator
     *
     * @return the next element in the iterator
     *
     * @throws IllegalStateException if there are no more elements in the iterator
     */
    Datum next();
}
