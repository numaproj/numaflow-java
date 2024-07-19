package io.numaproj.numaflow.batchmapper;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

public class BatchResponse {
    @Getter
    private final String id;
    private final List<Message> messages;

    public BatchResponse(String id) {
        this.id = id;
        this.messages = new ArrayList<>();
    }

    public BatchResponse append(Message msg) {
        this.messages.add(msg);
        return this;
    }

    public List<Message> getItems() {
        return messages;
    }
}
