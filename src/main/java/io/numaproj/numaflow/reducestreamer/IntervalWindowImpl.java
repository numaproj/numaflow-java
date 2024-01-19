package io.numaproj.numaflow.reducestreamer;

import io.numaproj.numaflow.reducestreamer.model.IntervalWindow;
import lombok.AllArgsConstructor;

import java.time.Instant;

@AllArgsConstructor
class IntervalWindowImpl implements IntervalWindow {
    private final Instant startTime;
    private final Instant endTime;

    @Override
    public Instant getStartTime() {
        return this.startTime;
    }

    @Override
    public Instant getEndTime() {
        return this.endTime;
    }
}
