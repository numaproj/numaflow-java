package io.numaproj.numaflow.sourcer;

import lombok.AllArgsConstructor;

import java.util.List;

/**
 * AckRequestImpl is the implementation of AckRequest.
 */
@AllArgsConstructor
class AckRequestImpl implements AckRequest {
    private final List<Offset> offsets;

    @Override
    public List<Offset> getOffsets() {
        return this.offsets;
    }
}
