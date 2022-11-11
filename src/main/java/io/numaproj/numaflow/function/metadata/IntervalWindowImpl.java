package io.numaproj.numaflow.function.metadata;

import java.time.Instant;

/**
 * IntervalWindowImpl implements IntervalWindow interface which will be passed
 * as metadata to reduce handlers
 */
public class IntervalWindowImpl implements IntervalWindow {

    private final Instant startTime;
    private final Instant endTime;

    public IntervalWindowImpl(Instant startTime, Instant endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public Instant GetStartTime() {
        return this.startTime;
    }

    @Override
    public Instant GetEndTime() {
        return this.endTime;
    }
}
