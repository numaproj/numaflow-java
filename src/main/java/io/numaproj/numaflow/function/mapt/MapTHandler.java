package io.numaproj.numaflow.function.mapt;

import io.numaproj.numaflow.function.MessageT;
import io.numaproj.numaflow.function.v1.Udfunction;

/**
 * Interface of map function implementation.
 */
public interface MapTHandler {

    // Function to process each coming message
    MessageT[] HandleDo(String key, Udfunction.Datum datum);
}
