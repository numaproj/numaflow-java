package io.numaproj.numaflow.sourcer;

import io.numaproj.numaflow.shared.NackOptions;
import lombok.AllArgsConstructor;

import java.util.List;

/**
 * NackRequestImpl is the implementation of NackRequest.
 */
@AllArgsConstructor
class NackRequestImpl implements NackRequest {
    private final List<Offset> offsets;
    private final NackOptions nackOptions;

    @Override
    public List<Offset> getOffsets() {
        return this.offsets;
    }

    @Override
    public NackOptions getNackOptions() {
        return this.nackOptions;
    }
}
