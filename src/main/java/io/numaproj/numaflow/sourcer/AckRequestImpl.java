package io.numaproj.numaflow.sourcer;

import lombok.AllArgsConstructor;

/**
 * AckRequestImpl is the implementation of AckRequest.
 */
@AllArgsConstructor
class AckRequestImpl implements AckRequest {
    private final Offset offset;

    @Override
    public Offset getOffset() {
        return this.offset;
    }
}
