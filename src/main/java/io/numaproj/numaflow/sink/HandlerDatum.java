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

    // poison packet for reduce stream, to indicate EOF
    public static HandlerDatum EOF() {
        return new HandlerDatum(null, null, null, null, true);
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
