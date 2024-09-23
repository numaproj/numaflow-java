package io.numaproj.numaflow.info;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Please exercise cautions when updating the values below because the exact same values are defined in other Numaflow SDKs
 * to form a contract between server and clients.
 */
public enum Language {
    GO("go"),
    PYTHON("python"),
    JAVA("java"),
    RUST("rust");

    private final String name;

    Language(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }
}
