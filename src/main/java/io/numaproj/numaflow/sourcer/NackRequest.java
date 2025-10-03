package io.numaproj.numaflow.sourcer;


import java.util.List;

/**
 * NackRequest request for negatively acknowledging messages.
 */
public interface NackRequest {
    /**
     * @return the list of offsets to be negatively acknowledged.
     */
    List<Offset> getOffsets();
}
