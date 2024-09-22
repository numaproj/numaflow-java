package io.numaproj.numaflow.sourcer;


/**
 * AckRequest request for acknowledging messages.
 */
public interface AckRequest {
    /**
     * @return the offset to be acknowledged
     */
    Offset getOffset();
}
