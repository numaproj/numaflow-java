package io.numaproj.numaflow.sink;

import java.util.List;

/**
 * Interface of sink function implementation.
 */
public interface SinkHandler {
    // Function to process a list of coming messages
    List<Response> HandleDo(SinkDatumStream datumStream);
}
