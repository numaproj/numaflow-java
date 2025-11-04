package io.numaproj.numaflow.sinker;

import lombok.Builder;
import lombok.Getter;

import java.util.HashMap;

@Getter
@Builder
public class KeyValueGroup {
    private final HashMap<String, byte[]> keyValue;
}
