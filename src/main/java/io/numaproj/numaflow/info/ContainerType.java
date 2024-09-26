package io.numaproj.numaflow.info;

import com.fasterxml.jackson.annotation.JsonValue;

public enum ContainerType {
    SOURCER("sourcer"),
    SOURCE_TRANSFORMER("sourcetransformer"),
    SINKER("sinker"),
    MAPPER("mapper"),
    REDUCER("reducer"),
    REDUCE_STREAMER("reducestreamer"),
    SESSION_REDUCER("sessionreducer"),
    SIDEINPUT("sideinput"),
    FBSINKER("fb-sinker");

    private final String name;

    ContainerType(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }
}
