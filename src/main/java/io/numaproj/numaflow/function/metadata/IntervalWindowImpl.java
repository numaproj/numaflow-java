package io.numaproj.numaflow.function.metadata;

import java.time.Instant;

public class IntervalWindowImpl implements IntervalWindow{

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
