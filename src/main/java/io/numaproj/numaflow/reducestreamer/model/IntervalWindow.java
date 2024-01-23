package io.numaproj.numaflow.reducestreamer.model;

import java.time.Instant;

/**
 * IntervalWindow contains methods to get the information for a given interval window.
 */
public interface IntervalWindow {
    /**
     * method to get the start time of the interval window
     *
     * @return start time of the window
     */
    Instant getStartTime();

    /**
     * method to get the end time of the interval window
     *
     * @return end time of the window
     */
    Instant getEndTime();
}
