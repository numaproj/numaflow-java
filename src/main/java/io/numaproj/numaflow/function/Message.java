package io.numaproj.numaflow.function;

import lombok.Getter;

/**
 * Message is used to wrap the data return by UDF functions.
 */

@Getter
public class Message {
    public static final String DROP = "U+005C__DROP__";

    private final String[] keys;
    private final byte[] value;
    private final String[] tags;

    // used to create message with keys, value and tags(used for conditional forwarding)
    public Message(byte[] value, String[] keys, String[] tags) {
        this.keys = keys;
        this.value = value;
        this.tags = tags;
    }

    // used to create Message with value.
    public Message(byte[] value) {
        this(value, null, null);
    }

    // used to create Message with keys and value.
    public Message(byte[] value, String[] keys) {
        this(value, keys, null);
    }

    // creates a Message to be dropped
    public static Message toDrop() {
        return new Message(null, null, new String[]{DROP});
    }
}
