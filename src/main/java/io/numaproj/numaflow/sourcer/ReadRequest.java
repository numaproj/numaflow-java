package io.numaproj.numaflow.sourcer;

import java.time.Instant;

/**
 * ReadRequest request for reading messages from source.
 */
public interface ReadRequest {
    /**
     * @return the number of messages to be read
     */
    long getCount();

    /**
     * @return the timeout for reading messages
     */
    Instant getTimeout();
}
