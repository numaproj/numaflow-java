package io.numaproj.numaflow.batchmapper;


import lombok.AllArgsConstructor;

import java.time.Instant;
import java.util.Map;

@AllArgsConstructor
class HandlerDatum implements Datum {

    static final HandlerDatum EOF_DATUM = new HandlerDatum(null, null, null, null, null, null);
    private String[] keys;
    private byte[] value;
    private Instant watermark;
    private Instant eventTime;
    private String id;
    private Map<String, String> headers;

    @Override
    public String[] getKeys() {
        return keys;
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

    @Override
    public Map<String, String> getHeaders() {
        return this.headers;
    }

}
