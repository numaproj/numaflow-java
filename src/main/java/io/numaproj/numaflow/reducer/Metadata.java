package io.numaproj.numaflow.reducer;

/**
 * Metadata contains methods to get the metadata for the reduce operation.
 */
public interface Metadata {
    /**
     * method to get the interval window.
     *
     * @return IntervalWindow which has the window information
     */
    IntervalWindow getIntervalWindow();
}

