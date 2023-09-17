package io.numaproj.numaflow.sourcer;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.Instant;

/**
 * ReadRequest is used to wrap the request for reading messages from source.
 */
@Getter
@AllArgsConstructor
class ReadRequestImpl implements ReadRequest {
    long count;
    Instant timeout;
}
