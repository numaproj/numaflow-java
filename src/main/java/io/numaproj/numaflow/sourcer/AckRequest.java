package io.numaproj.numaflow.sourcer;



/**
 * AckRequest request for acknowledging messages.
 */
public interface AckRequest {
    /**
     * @return the offsets to be acknowledged
     */
    Offset getOffset();
}
