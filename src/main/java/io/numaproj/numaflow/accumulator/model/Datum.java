package io.numaproj.numaflow.accumulator.model;

import java.time.Instant;
import java.util.Map;

/**
 * Datum contains methods to get the payload and metadata information.
 */
public interface Datum {
    /**
     * method to get the payload value
     *
     * @return returns the payload value in byte array
     */
    byte[] getValue();

    /**
     * method to get the keys
     *
     * @return returns the keys in the form of string array
     */
    String[] getKeys();

    /**
     * method to get the event time of the payload
     *
     * @return returns the event time of the payload
     */
    Instant getEventTime();

    /**
     * method to get the watermark information
     *
     * @return returns the watermark
     */
    Instant getWatermark();

    /**
     * method to get the headers information of the payload
     *
     * @return returns the headers in the form of key value pair
     */
    Map<String, String> getHeaders();

    /**
     * method to get the ID of the payload, ID is used for deduplication
     *
     * @return returns the ID of the payload
     */
    String getID();
}
