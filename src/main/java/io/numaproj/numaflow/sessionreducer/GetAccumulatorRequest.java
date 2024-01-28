package io.numaproj.numaflow.sessionreducer;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * GetAccumulatorRequest is sent by supervisor actor to inform a session reducer actor that
 * the window is to be merged with other windows.
 * <p>
 * "I am working on a merge task (mergeTaskId),
 * and you are one of the windows to be merged.
 * Please give me your accumulator."
 */
@AllArgsConstructor
@Getter
class GetAccumulatorRequest {
    private final String mergeTaskId;
}
