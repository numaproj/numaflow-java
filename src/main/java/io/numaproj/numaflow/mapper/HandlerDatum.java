package io.numaproj.numaflow.mapper;


import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.Instant;

@Getter
@AllArgsConstructor
class HandlerDatum implements Datum {

    private byte[] value;
    private Instant watermark;
    private Instant eventTime;


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

}
