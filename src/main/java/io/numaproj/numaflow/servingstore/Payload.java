package io.numaproj.numaflow.servingstore;

import lombok.Getter;
import lombok.AllArgsConstructor;

/**
 * Payload is each independent result stored in the Store per origin for a given ID.
 */
@Getter
@AllArgsConstructor
public class Payload {
    // origin is the name of the vertex that produced the payload.
    private String origin;
    // value is the byte array of the payload.
    private byte[] value;
}
