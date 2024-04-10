package io.numaproj.numaflow.sourcer;

import lombok.Getter;

import java.time.Instant;
import java.util.Map;

/**
 * Message is used to wrap the data returned by Sourcer.
 */

@Getter
public class Message {

    private final String[] keys;
    private final byte[] value;
    private final Offset offset;
    private final Instant eventTime;
    private final Map<String, String> headers;

    /**
     * used to create Message with value, offset and eventTime.
     *
     * @param value message value
     * @param offset message offset
     * @param eventTime message eventTime
     */
    public Message(byte[] value, Offset offset, Instant eventTime) {
        this(value, offset, eventTime, null, null);
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
        this(value, offset, eventTime, keys, null);
    }

    /**
     * used to create Message with value, offset, eventTime and headers.
     *
     * @param value message value
     * @param offset message offset
     * @param eventTime message eventTime
     * @param headers message headers
     */
    public Message(byte[] value, Offset offset, Instant eventTime, Map<String, String> headers) {
        this(value, offset, eventTime, null, headers);
    }

    /**
     * used to create Message with value, offset, eventTime, keys and headers.
     *
     * @param value message value
     * @param offset message offset
     * @param eventTime message eventTime
     * @param keys message keys
     * @param headers message headers
     */
    public Message(
            byte[] value,
            Offset offset,
            Instant eventTime,
            String[] keys,
            Map<String, String> headers) {
        this.value = value;
        this.offset = offset;
        this.eventTime = eventTime;
        this.keys = keys;
        this.headers = headers;
    }
}
