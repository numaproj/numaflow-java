package io.numaproj.numaflow.function;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

import java.util.List;

/**
 * MessageList is used to return the list of Message from UDF
 */

@Getter
@Builder(builderMethodName = "newBuilder")
public class MessageList {

    @Singular("addMessage")
    private List<Message> messages;

    public static class MessageListBuilder {
        public MessageListBuilder addMessages(List<Message> messages) {
            this.messages.addAll(messages);
            return this;
        }
    }
}
