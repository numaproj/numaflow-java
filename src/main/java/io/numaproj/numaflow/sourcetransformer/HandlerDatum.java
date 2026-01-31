package io.numaproj.numaflow.sourcetransformer;


import io.numaproj.numaflow.shared.SystemMetadata;
import io.numaproj.numaflow.shared.UserMetadata;
import lombok.AllArgsConstructor;

import java.time.Instant;
import java.util.Map;

@AllArgsConstructor
class HandlerDatum implements Datum {

    private byte[] value;
    private Instant watermark;
    private Instant eventTime;
    private Map<String, String> headers;
    private UserMetadata userMetadata;
    private SystemMetadata systemMetadata;


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

    @Override
    public UserMetadata getUserMetadata() {
        return this.userMetadata;
    }

    @Override
    public SystemMetadata getSystemMetadata() {
        return this.systemMetadata;
    }
}
