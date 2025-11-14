package io.numaproj.numaflow.sinker;

import lombok.Builder;
import lombok.Getter;

import java.util.HashMap;

/**
 * KeyValueGroup is a map of key-value pairs for a given group.
 * Used as part of {@link io.numaproj.numaflow.sinker.Message}.
 */
@Getter
@Builder
public class KeyValueGroup {
    private final HashMap<String, byte[]> keyValue;
}
