package io.numaproj.numaflow.function;


import lombok.AllArgsConstructor;

import java.time.Instant;

@AllArgsConstructor
public class HandlerDatum implements Datum {

    private byte[] value;
    private Instant watermark;
    private Instant eventTime;

    @Override
    public Instant getWatermark() {
        return this.watermark;
    }

    @Override
    public byte[] getValue() {
        return this.value;
    }

    @Override
    public Instant getEventTime() {
        return this.eventTime;
    }
}
