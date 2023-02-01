package io.numaproj.numaflow.function.metadata;

import java.time.Instant;

/**
 * IntervalWindow contains methods to get the information for a given interval window.
 */
public interface IntervalWindow {

    Instant GetStartTime();

    Instant GetEndTime();
}
