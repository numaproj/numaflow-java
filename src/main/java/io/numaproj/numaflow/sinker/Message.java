package io.numaproj.numaflow.sinker;

import lombok.Builder;
import lombok.Getter;

import java.util.HashMap;

/**
 * Message contains information that needs to be sent to the OnSuccess sink.
 * The message can be different from the original message that was sent to primary sink.
 */
@Getter
@Builder
public class Message {
    private final byte[] value;
    private final String key;
    private final HashMap<String, KeyValueGroup> userMetadata;
}


