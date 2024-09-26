package io.numaproj.numaflow.info;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * the string content matches the corresponding server info file name.
 * DO NOT change it unless the server info file name is changed.
 */
public enum ContainerType {
    Sourcer("sourcer"),
    Sourcetransformer("sourcetransformer"),
    Sinker("sinker"),
    Mapper("mapper"),
    Reducer("reducer"),
    Reducestreamer("reducestreamer"),
    Sessionreducer("sessionreducer"),
    Sideinput("sideinput"),
    Fbsinker("fb-sinker"),
    Unknown("unknown");

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
        return Unknown;
    }
}
