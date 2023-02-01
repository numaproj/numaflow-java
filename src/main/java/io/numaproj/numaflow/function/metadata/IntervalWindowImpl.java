package io.numaproj.numaflow.function.metadata;

import java.time.Instant;
import lombok.AllArgsConstructor;

/**
 * IntervalWindowImpl implements IntervalWindow interface which will be passed as metadata to reduce
 * handlers
 */
@AllArgsConstructor
public class IntervalWindowImpl implements IntervalWindow {

    private final Instant startTime;
    private final Instant endTime;

    @Override
    public Instant GetStartTime() {
        return this.startTime;
    }

    @Override
    public Instant GetEndTime() {
        return this.endTime;
    }
}
