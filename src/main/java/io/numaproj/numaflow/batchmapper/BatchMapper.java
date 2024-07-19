package io.numaproj.numaflow.batchmapper;

/**
 * Mapper exposes method for performing map operation.
 * Implementations should override the processMessage method
 * which will be used for processing the input messages
 */

public abstract class BatchMapper {
    /**
     * method which will be used for processing messages.
     *
     * @param datumStream current message to be processed
     *
     * @return MessageList which contains output from map
     */
    public abstract BatchResponses processMessage(DatumIterator datumStream);
}
