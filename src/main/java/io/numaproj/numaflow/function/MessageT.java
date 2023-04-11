package io.numaproj.numaflow.function;

import lombok.Builder;
import lombok.Getter;

import java.time.Instant;

import static io.numaproj.numaflow.function.Message.DROP;

/**
 * MessageT is used to wrap the data return by UDF functions. Compared with Message, MessageT
 * contains one more field, the event time, usually extracted from the payload.
 */
@Getter
@Builder(builderMethodName = "newBuilder")
public class MessageT {
    private final String[] keys;
    private final byte[] value;
    private final String[] tags;
    private Instant eventTime;

    // creates a MessageT to be dropped
    public static MessageT toDrop() {
        return MessageT.newBuilder().tags(new String[]{DROP}).build();
    }
}
