package io.numaproj.numaflow.function;

import io.numaproj.numaflow.function.interfaces.DatumMetadata;
import lombok.AllArgsConstructor;

@AllArgsConstructor
class HandlerDatumMetadata implements DatumMetadata {
    private String id;
    private long numDelivered;

    public String getId() {
        return this.id;
    }

    public long getNumDelivered() {
        return this.numDelivered;
    }
}
