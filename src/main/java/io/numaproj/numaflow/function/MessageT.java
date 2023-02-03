package io.numaproj.numaflow.function;

import static io.numaproj.numaflow.function.Message.ALL;
import static io.numaproj.numaflow.function.Message.DROP;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * MessageT is used to wrap the data return by UDF functions. Compared with Message, MessageT
 * contains one more field, the event time, usually extracted from the payload.
 */
@AllArgsConstructor
@Getter
public class MessageT {

    private Instant eventTime;
    private final String key;
    private final byte[] value;

    // creates a MessageT to be dropped
    public static MessageT toDrop() {
        return new MessageT(Instant.MIN, DROP, new byte[0]);
    }

    // creates a MessageT that will forward to all
    public static MessageT toAll(Instant eventTime, byte[] value) {
        return new MessageT(eventTime, ALL, value);
    }

    // creates a MessageT that will forward to specified "to"
    public static MessageT to(Instant eventTime, String to, byte[] value) {
        return new MessageT(eventTime, to, value);
    }
}
