package io.numaproj.numaflow.servingstore;

import java.util.List;

/**
 * PutDatum interface exposes methods to retrieve data from the Put RPC.
 */
public interface PutDatum {
    /**
     * Returns the ID.
     *
     * @return the ID
     */
    String ID();

    /**
     * Returns the list of payloads.
     *
     * @return the list of payloads
     */
    List<Payload> Payloads();
}