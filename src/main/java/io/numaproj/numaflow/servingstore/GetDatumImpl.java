package io.numaproj.numaflow.servingstore;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class GetDatumImpl implements GetDatum {
    private final String id;

    /**
     * Returns the ID.
     *
     * @return the ID
     */
    @Override
    public String ID() {
        return id;
    }
}