package io.numaproj.numaflow.servingstore;

/**
 * Payload is each independent result stored in the Store per origin for a given
 * ID.
 */
public class Payload {
    private String origin;
    private byte[] value;

    /**
     * Creates a new Payload from the given value.
     *
     * @param origin the origin name
     * @param value  the value of the payload
     */
    public Payload(String origin, byte[] value) {
        this.origin = origin;
        this.value = value;
    }

    /**
     * Returns the value of the Payload.
     *
     * @return the value of the payload
     */
    public byte[] getValue() {
        return value;
    }

    /**
     * Returns the origin name.
     *
     * @return the origin name
     */
    public String getOrigin() {
        return origin;
    }
}