package io.numaproj.numaflow.sink;

import io.numaproj.numaflow.sink.v1.Udsink;

/**
 * Interface of sink function implementation.
 */
public interface SinkHandler {
    // Function to process a list of coming messages
    Response[] HandleDo(Udsink.Datum[] datumList);
}
