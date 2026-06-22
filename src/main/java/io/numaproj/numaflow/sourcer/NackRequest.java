package io.numaproj.numaflow.sourcer;

import io.numaproj.numaflow.shared.NackOptions;

import java.util.List;

/**
 * NackRequest request for negatively acknowledging messages.
 */
public interface NackRequest {
    /**
     * @return the list of offsets to be negatively acknowledged.
     */
    List<Offset> getOffsets();

    /**
     * @return the redelivery options for this nack, or null if none were provided.
     */
    NackOptions getNackOptions();
}
