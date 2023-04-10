package io.numaproj.numaflow.function;

import lombok.Builder;
import lombok.Getter;

/**
 * Message is used to wrap the data return by UDF functions.
 */

@Getter
@Builder
public class Message {
    public static final String DROP = "U+005C__DROP__";

    private final String[] keys;
    private final byte[] value;
    private final String[] tags;

    // creates a Message to be dropped
    public static Message toDrop() {
        return Message.builder().tags(new String[]{DROP}).build();
    }
}
