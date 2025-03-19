package io.numaproj.numaflow.accumulator;


import io.numaproj.numaflow.accumulator.model.Datum;
import lombok.AllArgsConstructor;

import java.time.Instant;
import java.util.Map;

@AllArgsConstructor
class HandlerDatum implements Datum {
    private String[] keys;
    private byte[] value;
    private Instant watermark;
    private Instant eventTime;
    private Map<String, String> headers;
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
    public String[] getKeys() {
        return this.keys;
    }

    @Override
    public Instant getEventTime() {
        return this.eventTime;
    }

    @Override
    public Map<String, String> getHeaders() {
        return this.headers;
    }

    @Override
    public String getID() {
        return this.id;
    }
}
