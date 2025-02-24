package io.numaproj.numaflow.servingstore;

import java.util.List;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PutDatumImpl implements PutDatum {
    private final String id;
    private final List<Payload> payloads;

    /**
     * Returns the ID.
     *
     * @return the ID
     */
    @Override
    public String ID() {
        return id;
    }

    /**
     * Returns the list of payloads.
     *
     * @return the list of payloads
     */
    @Override
    public List<Payload> Payloads() {
        return payloads;
    }
}