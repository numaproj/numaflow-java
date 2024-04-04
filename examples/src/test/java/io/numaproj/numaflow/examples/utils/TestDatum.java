package io.numaproj.numaflow.examples.utils;

import io.numaproj.numaflow.mapper.Datum;
import lombok.Builder;

import java.time.Instant;
import java.util.Map;

@Builder
public class TestDatum implements Datum, io.numaproj.numaflow.sourcetransformer.Datum {
    private byte[] value;
    private Instant eventTime;
    private Instant watermark;
    private Map<String, String> headers;

    @Override
    public byte[] getValue() {
        return value;
    }

    @Override
    public Instant getEventTime() {
        return eventTime;
    }

    @Override
    public Instant getWatermark() {
        return watermark;
    }

    @Override
    public Map<String, String> getHeaders() {
        return headers;
    }
}
