package io.numaproj.numaflow.batchmapper;

import java.util.ArrayList;
import java.util.List;

public class BatchResponses {
    private final List<BatchResponse> batchResponses;

    public BatchResponses() {
        this.batchResponses = new ArrayList<>();
    }

    public BatchResponses append(BatchResponse batchResponse) {
        this.batchResponses.add(batchResponse);
        return this;
    }

    public List<BatchResponse> getItems() {
        return batchResponses;
    }
}
