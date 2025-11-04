package io.numaproj.numaflow.sinker;

import lombok.Builder;
import lombok.Getter;

import java.util.HashMap;

@Getter
@Builder
public class OnSuccessMessage {
    private final byte[] value;
    private final String key;
    private final HashMap<String, KeyValueGroup> userMetadata;
}


