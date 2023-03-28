package io.numaproj.numaflow.function;

import java.time.Instant;

public interface Datum {
    public byte[] getValue();

    public Instant getEventTime();

    public Instant getWatermark();
}
