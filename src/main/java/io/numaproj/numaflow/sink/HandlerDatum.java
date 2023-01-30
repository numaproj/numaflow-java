package io.numaproj.numaflow.sink;


import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.time.Instant;

@AllArgsConstructor
@NoArgsConstructor
public class HandlerDatum implements Datum {

    private byte[] value;
    private Instant watermark;
    private Instant eventTime;
    private String id;

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

    @Override
    public String getId() {
        return id;
    }
}
