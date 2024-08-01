package io.numaproj.numaflow.batchmapper;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * BatchResponse is used to collect and manage a batch of Message objects.
 */
public class BatchResponse {
    @Getter
    private final String id;
    private final List<Message> messages;

    /**
     * Constructs a BatchResponse with a specified ID.
     *
     * @param id the unique identifier for this batch response
     */
    public BatchResponse(String id) {
        this.id = id;
        this.messages = new ArrayList<>();
    }

    /**
     * Appends a Message to the batch.
     *
     * @param msg the Message to be added to the batch
     * @return the current BatchResponse instance for method chaining
     */
    public BatchResponse append(Message msg) {
        this.messages.add(msg);
        return this;
    }

    /**
     * Retrieves the list of Messages in the batch.
     *
     * @return the list of Messages
     */
    public List<Message> getItems() {
        return messages;
    }
}
