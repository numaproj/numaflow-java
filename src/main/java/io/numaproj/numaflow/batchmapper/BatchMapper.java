package io.numaproj.numaflow.batchmapper;

/**
 * BatchMapper exposes method for performing batch map operation.
 * Implementations should override the processMessage method
 * which will be used for processing the input messages
 */

public abstract class BatchMapper {
    /**
     * method which will be used for processing messages. Please implement the interface to ensure that each message generates a corresponding BatchResponse object with a matching ID.
     *
     * @param datumStream current message to be processed
     *
     * @return BatchResponses which contains output from batch map
     */
    public abstract BatchResponses processMessage(DatumIterator datumStream);
}
