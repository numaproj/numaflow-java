package io.numaproj.numaflow.sinker;

import io.numaproj.numaflow.shared.SystemMetadata;
import io.numaproj.numaflow.shared.UserMetadata;
import lombok.AllArgsConstructor;

import java.time.Instant;
import java.util.Map;

@AllArgsConstructor
class HandlerDatum implements Datum {

    // EOF_DATUM is used to indicate the end of the stream.
    static final HandlerDatum EOF_DATUM = new HandlerDatum(null, null, null, null, null, null, null, null);
    private String[] keys;
    private byte[] value;
    private Instant watermark;
    private Instant eventTime;
    private String id;
    private Map<String, String> headers;
    private UserMetadata userMetadata;
    private SystemMetadata systemMetadata;

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

    @Override
    public UserMetadata getUserMetadata() {
        return this.userMetadata;
    }

    @Override
    public SystemMetadata getSystemMetadata() {
        return this.systemMetadata;
    }
}
