package io.numaproj.numaflow.servingstore;

/**
 * GetDatum is the interface to expose methods to retrieve data from the Get
 * RPC.
 */
public interface GetDatum {
    /**
     * Returns the ID.
     *
     * @return the ID
     */
    String ID();
}