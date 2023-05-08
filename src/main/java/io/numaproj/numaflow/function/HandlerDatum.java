package io.numaproj.numaflow.function;


import io.numaproj.numaflow.function.interfaces.Datum;
import io.numaproj.numaflow.function.interfaces.DatumMetadata;
import lombok.AllArgsConstructor;

import java.time.Instant;

@AllArgsConstructor
class HandlerDatum implements Datum {

    private byte[] value;
    private Instant watermark;
    private Instant eventTime;

    private DatumMetadata datumMetadata;

    @Override
    public Instant getWatermark() {
        return this.watermark;
    }

    @Override
    public byte[] getValue() {
        return this.value;
    }

    @Override
    public Instant getEventTime() {
        return this.eventTime;
    }

    @Override
    public DatumMetadata getDatumMetadata() {
        return this.datumMetadata;
    }
}
