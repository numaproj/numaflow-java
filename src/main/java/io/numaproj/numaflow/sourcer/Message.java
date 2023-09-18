package io.numaproj.numaflow.sourcer;

import lombok.Getter;

import java.time.Instant;

/**
 * Message is used to wrap the data returned by Sourcer.
 */

@Getter
public class Message {

    private final String[] keys;
    private final byte[] value;
    private final Offset offset;
    private final Instant eventTime;

    /**
     * used to create Message with value, offset and eventTime.
     *
     * @param value message value
     * @param offset message offset
     * @param eventTime message eventTime
     */
    public Message(byte[] value, Offset offset, Instant eventTime) {
        this(value, offset, eventTime, null);
    }

    /**
     * used to create Message with value, offset, eventTime and keys.
     *
     * @param value message value
     * @param offset message offset
     * @param eventTime message eventTime
     * @param keys message keys
     */
    public Message(byte[] value, Offset offset, Instant eventTime, String[] keys) {
        this.value = value;
        this.offset = offset;
        this.eventTime = eventTime;
        this.keys = keys;
    }
}
