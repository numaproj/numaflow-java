package io.numaproj.numaflow.sourcer;

import lombok.AllArgsConstructor;

import java.util.List;

/**
 * NackRequestImpl is the implementation of NackRequest.
 */
@AllArgsConstructor
class NackRequestImpl implements NackRequest {
    private final List<Offset> offsets;

    @Override
    public List<Offset> getOffsets() {
        return this.offsets;
    }
}
