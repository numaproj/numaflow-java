package io.numaproj.numaflow.function.types;

import lombok.Getter;

import java.time.Instant;

import static io.numaproj.numaflow.function.types.Message.DROP;

/**
 * MessageT is used to wrap the data return by UDF functions. Compared with Message, MessageT
 * contains one more field, the event time, usually extracted from the payload.
 */
@Getter
public class MessageT {
    private final String[] keys;
    private final byte[] value;
    private final Instant eventTime;
    private final String[] tags;

    /**
     * used to create MessageT with value, eventTime, keys and tags(used for conditional forwarding)
     *
     * @param value message value
     * @param eventTime message eventTime
     * @param keys message keys
     * @param tags message tags which will be used for conditional forwarding
     */
    public MessageT(byte[] value, Instant eventTime, String[] keys, String[] tags) {
        this.keys = keys;
        this.value = value;
        this.tags = tags;
        this.eventTime = eventTime;
    }

    /**
     * used to create MessageT with value and eventTime.
     *
     * @param value message value
     * @param eventTime message eventTime
     */
    public MessageT(byte[] value, Instant eventTime) {
        this(value, eventTime, null, null);
    }

    /**
     * used to create MessageT with value, eventTime and keys
     *
     * @param value message value
     * @param eventTime message eventTime
     * @param keys message keys
     */
    public MessageT(byte[] value, Instant eventTime, String[] keys) {
        this(value, eventTime, keys, null);
    }

    /**
     *  creates a MessageT which will be dropped
     *
     * @return returns the MessageT which will be dropped
     */
    public static MessageT toDrop() {
        return new MessageT(new byte[0], Instant.MIN, null, new String[]{DROP});
    }
}
