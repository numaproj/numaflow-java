package io.numaproj.numaflow.batchmapper;

import java.util.ArrayList;
import java.util.List;

/**
 * BatchResponses is used to send a response from the batch map functions.
 * It contains a list of BatchResponse objects.
 */
public class BatchResponses {
    private final List<BatchResponse> batchResponses;

    /**
     * Constructs an empty BatchResponses object.
     */
    public BatchResponses() {
        this.batchResponses = new ArrayList<>();
    }

    /**
     * Appends a BatchResponse to the list of batchResponses.
     *
     * @param batchResponse the BatchResponse to be added
     *
     * @return the current BatchResponses object
     */
    public BatchResponses append(BatchResponse batchResponse) {
        this.batchResponses.add(batchResponse);
        return this;
    }

    /**
     * Retrieves the list of BatchResponse objects.
     *
     * @return the list of BatchResponse objects
     */
    public List<BatchResponse> getItems() {
        return batchResponses;
    }
}
