package io.numaproj.numaflow.sourcer;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

/**
 * AckRequestImpl is the implementation of AckRequest.
 */
@Getter
@AllArgsConstructor
class AckRequestImpl implements AckRequest {
    private final List<Offset> offsets;
}
