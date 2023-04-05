package io.numaproj.numaflow.sink;

import java.time.Instant;

public interface Datum {
    public abstract String[] getKeys();

    public abstract byte[] getValue();

    public abstract Instant getEventTime();

    public abstract Instant getWatermark();

    public abstract String getId();
}
