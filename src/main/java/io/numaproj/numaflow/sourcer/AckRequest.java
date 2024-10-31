package io.numaproj.numaflow.sourcer;


import java.util.List;

/**
 * AckRequest request for acknowledging messages.
 */
public interface AckRequest {
    /**
     * @return the list of offsets to be acknowledged
     */
    List<Offset> getOffsets();
}
