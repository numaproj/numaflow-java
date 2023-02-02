package io.numaproj.numaflow.function.mapt;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.MessageT;

/**
 * Interface of mapT function implementation.
 */
public interface MapTHandler {

    // Function to process each coming message
    MessageT[] HandleDo(String key, Datum datum);
}
