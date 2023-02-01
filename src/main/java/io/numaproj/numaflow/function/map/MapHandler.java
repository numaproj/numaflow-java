package io.numaproj.numaflow.function.map;

import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.v1.Udfunction;

/**
 * Interface of map function implementation.
 */
public interface MapHandler {

    // Function to process each coming message
    Message[] HandleDo(String key, Udfunction.Datum datum);
}
