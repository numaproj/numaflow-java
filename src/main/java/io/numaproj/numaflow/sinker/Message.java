package io.numaproj.numaflow.sinker;

import io.numaproj.numaflow.shared.UserMetadata;
import lombok.Builder;
import lombok.Getter;

/**
 * Message contains information that needs to be sent to the OnSuccess sink.
 * The message can be different from the original message that was sent to primary sink.
 */
@Getter
@Builder
public class Message {
    private final byte[] value;
    private final String key;
    private final UserMetadata userMetadata;
}


