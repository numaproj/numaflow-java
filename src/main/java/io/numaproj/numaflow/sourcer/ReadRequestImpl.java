package io.numaproj.numaflow.sourcer;

import lombok.AllArgsConstructor;
import java.time.Duration;

/**
 * ReadRequest is used to wrap the request for reading messages from source.
 */
@AllArgsConstructor
class ReadRequestImpl implements ReadRequest {
    long count;
    Duration timeout;

    @Override
    public long getCount() {
        return this.count;
    }

    @Override
    public Duration getTimeout() {
        return this.timeout;
    }
}
