package io.numaproj.numaflow.sourcetransformer;

import lombok.Getter;

import java.time.Instant;


/**
 * Message is used to wrap the data return by SourceTransformer functions.
 */
@Getter
public class Message {
    public static final String DROP = "U+005C__DROP__";
    // Watermark are at millisecond granularity, hence we use epoch(0) - 1 to indicate watermark is not available.
    // EventTimeForDrop is used to indicate that the message is dropped hence, excluded from watermark calculation
    private static final Instant EventTimeForDrop = Instant.ofEpochMilli(-1);
    private final String[] keys;
    private final byte[] value;
    private final Instant eventTime;
    private final String[] tags;

    /**
     * used to create Message with value, eventTime, keys and tags(used for conditional forwarding)
     *
     * @param value message value
     * @param eventTime message eventTime
     * @param keys message keys
     * @param tags message tags which will be used for conditional forwarding
     */
    public Message(byte[] value, Instant eventTime, String[] keys, String[] tags) {
        this.keys = keys;
        this.value = value;
        this.tags = tags;
        this.eventTime = eventTime;
    }

    /**
     * used to create Message with value and eventTime.
     *
     * @param value message value
     * @param eventTime message eventTime
     */
    public Message(byte[] value, Instant eventTime) {
        this(value, eventTime, null, null);
    }

    /**
     * used to create Message with value, eventTime and keys
     *
     * @param value message value
     * @param eventTime message eventTime
     * @param keys message keys
     */
    public Message(byte[] value, Instant eventTime, String[] keys) {
        this(value, eventTime, keys, null);
    }

    /**
     * creates a Message which will be dropped
     *
     * @return returns the Message which will be dropped
     */
    public static Message toDrop() {
        return new Message(
                new byte[0],
                EventTimeForDrop,
                null,
                new String[]{DROP});
    }
}
