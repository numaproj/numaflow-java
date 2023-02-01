package io.numaproj.numaflow.sink;

import lombok.AllArgsConstructor;

import java.time.Instant;

@AllArgsConstructor
public class HandlerDatum implements Datum {

    private byte[] value;
    private Instant watermark;
    private Instant eventTime;
    private String id;
    private Boolean eof;

    public HandlerDatum(Boolean eof) {
        this(null, null, null, null, true);
    }

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
