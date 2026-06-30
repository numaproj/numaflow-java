package io.numaproj.numaflow.sourcer;

import lombok.AllArgsConstructor;

import java.util.List;

/**
 * NackRequestImpl is the implementation of NackRequest.
 */
@AllArgsConstructor
class NackRequestImpl implements NackRequest {
    private final List<NackOffset> offsets;

    @Override
    public List<NackOffset> getOffsets() {
        return this.offsets;
    }
}
