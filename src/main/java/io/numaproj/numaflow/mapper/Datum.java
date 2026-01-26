package io.numaproj.numaflow.mapper;


import io.numaproj.numaflow.shared.SystemMetadata;
import io.numaproj.numaflow.shared.UserMetadata;

import java.time.Instant;
import java.util.Map;

/**
 * Datum contains methods to get the payload information.
 */

public interface Datum {
    /**
     * method to get the payload value
     *
     * @return returns the payload value in byte array
     */
    byte[] getValue();

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
     * method to get the metadata information added by the user.
     * It can be appended to and passed downstream.
     *
     * @return returns the UserMetadata object
     */
    UserMetadata getUserMetadata();

    /**
     * method to get the read-only system metadata information
     *
     * @return returns the SystemMetadata object
     */
    SystemMetadata getSystemMetadata();
}
