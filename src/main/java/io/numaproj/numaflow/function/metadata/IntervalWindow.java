package io.numaproj.numaflow.function.metadata;

import java.time.Instant;

public interface IntervalWindow {
    Instant GetStartTime();

    Instant GetEndTime();
}
