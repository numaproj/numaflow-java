package io.numaproj.numaflow.function;

import lombok.Getter;

import java.time.Instant;

import static io.numaproj.numaflow.function.Message.DROP;

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

    // used to create MessageT with eventTime, keys, value and tags.
    public MessageT(byte[] value, Instant eventTime, String[] keys, String[] tags) {
        this.keys = keys;
        this.value = value;
        this.tags = tags;
        this.eventTime = eventTime;
    }

    // used to create MessageT with eventTime and value.
    public MessageT(byte[] value, Instant eventTime) {
        this(value, eventTime, null, null);
    }

    // used to create MessageT with eventTime, keys and value.
    public MessageT(byte[] value, Instant eventTime, String[] keys) {
        this(value, eventTime, keys, null);
    }

    // creates a MessageT to be dropped
    public static MessageT toDrop() {
        return new MessageT(null, null, null, new String[]{DROP});
    }
}
