package io.numaproj.numaflow.function;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class HandlerDatumMetadata implements DatumMetadata {
    private String id;
    private long numDelivered;

    public String getId() {
        return this.id;
    }

    public long getNumDelivered() {
        return this.numDelivered;
    }
}
