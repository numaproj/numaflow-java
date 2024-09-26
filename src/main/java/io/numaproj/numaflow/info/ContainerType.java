package io.numaproj.numaflow.info;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * the string content matches the corresponding server info file name.
 * DO NOT change it unless the server info file name is changed.
 */
public enum ContainerType {
    SOURCER("sourcer"),
    SOURCE_TRANSFORMER("sourcetransformer"),
    SINKER("sinker"),
    MAPPER("mapper"),
    REDUCER("reducer"),
    REDUCE_STREAMER("reducestreamer"),
    SESSION_REDUCER("sessionreducer"),
    SIDEINPUT("sideinput"),
    FBSINKER("fb-sinker"),
    UNKNOWN("unknown");

    private final String name;

    ContainerType(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }

    public static ContainerType fromString(String text) {
        for (ContainerType b : ContainerType.values()) {
            if (b.name.equalsIgnoreCase(text)) {
                return b;
            }
        }
        return UNKNOWN;
    }
}
