package io.numaproj.numaflow.sideinput;

import lombok.Getter;

/**
 * Message is used to wrap the data returned by Side Input Retriever.
 */

@Getter
public class Message {
    private final byte[] value;

    /**
     * used to create Message with value.
     *
     * @param value message value
     */
    public Message(byte[] value) {
        this.value = value;
    }
}
