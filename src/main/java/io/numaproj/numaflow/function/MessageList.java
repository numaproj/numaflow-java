package io.numaproj.numaflow.function;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

import java.util.Collection;

/**
 * MessageList is used to return the list of Message from UDF
 */

@Getter
@Builder(builderMethodName = "newBuilder")
public class MessageList {

    @Singular("addMessage")
    private Iterable<Message> messages;

    public static class MessageListBuilder {
        public MessageListBuilder addMessages(Iterable<Message> messages) {
            this.messages.addAll((Collection<? extends Message>) messages);
            return this;
        }
    }
}
