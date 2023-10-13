package io.numaproj.numaflow.sourcetransformer;

import lombok.Getter;

import java.time.Instant;


/**
 * Message is used to wrap the data return by SourceTransformer functions.
 */
@Getter
public class Message {
    public static final String DROP = "U+005C__DROP__";
    // epoch second -86400 translates to UTC time 1969-12-31 00:00:00 +0000 UTC,
    // EventTimeForDrop is set to be slightly earlier than Unix epoch -1 (1969-12-31 23:59:59.999 +0000 UTC)
    // As -1 is used on Numaflow to indicate watermark is not available,
    // EventTimeForDrop is used to indicate that the message is dropped hence, excluded from watermark calculation
    public static final Instant EventTimeForDrop = Instant.ofEpochSecond(-86400);
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
