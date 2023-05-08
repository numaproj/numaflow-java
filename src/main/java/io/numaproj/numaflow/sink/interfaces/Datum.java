package io.numaproj.numaflow.sink.interfaces;

import java.time.Instant;

/**
 * Datum contains methods to get the payload information.
 */
public interface Datum {
    /**
     * method to get the payload keys
     * @return returns the datum keys.
     */
    public abstract String[] getKeys();
    /**
     * method to get the payload value
     * @return returns the payload value in byte array
     */
    public abstract byte[] getValue();

    /**
     * method to get the event time of the payload
     * @return returns the event time of the payload
     */
    public abstract Instant getEventTime();
    /**
     * method to get the watermark information
     * @return returns the watermark
     */
    public abstract Instant getWatermark();

    /**
     * method to get the ID for the Payload
     * @return returns the ID
     */
    public abstract String getId();
}
