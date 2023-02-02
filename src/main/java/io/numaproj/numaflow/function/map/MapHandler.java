package io.numaproj.numaflow.function.map;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.Message;

/**
 * Interface of map function implementation.
 */
public interface MapHandler {
    // Function to process each coming message
    Message[] HandleDo(String key, Datum datum);
}
