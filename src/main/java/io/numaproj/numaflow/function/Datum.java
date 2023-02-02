package io.numaproj.numaflow.function;

import java.time.Instant;

public interface Datum {
    public abstract byte[] getValue();

    public abstract Instant getEventTime();

    public abstract Instant getWatermark();
}
