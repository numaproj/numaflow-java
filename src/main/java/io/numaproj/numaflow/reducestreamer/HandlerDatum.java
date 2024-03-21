package io.numaproj.numaflow.reducestreamer;

import io.numaproj.numaflow.reducestreamer.model.Datum;
import lombok.AllArgsConstructor;

import java.time.Instant;
import java.util.Map;

@AllArgsConstructor
class HandlerDatum implements Datum {
    private byte[] value;
    private Instant watermark;
    private Instant eventTime;
    private Map<String, String> headers;

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
    public Map<String, String> getHeaders() {
        return this.headers;
    }
}
