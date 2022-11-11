package io.numaproj.numaflow.function.reduce;

import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.metadata.Metadata;

/**
 * Interface of reduce function implementation.
 */
public interface ReduceHandler {
    Message[] HandleDo(String key, ReduceDatumStream reduceDatumStream, Metadata md);
}
