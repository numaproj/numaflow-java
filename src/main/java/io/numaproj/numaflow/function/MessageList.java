package io.numaproj.numaflow.function;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * MessageList is used to return the list of Message from UDF
 */

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MessageList {
    private List<Message> messageList;

    private MessageList(MessageListBuilder messageListBuilder) {
        this.messageList = messageListBuilder.messageList;
    }

    public static MessageListBuilder builder() {
        return new MessageListBuilder();
    }

    public List<Message> getMessages() {
        return messageList;
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class MessageListBuilder {
        private final List<Message> messageList = new ArrayList<>();

        public MessageListBuilder addMessage(Message message) {
            this.messageList.add(message);
            return this;
        }

        public MessageListBuilder addMessages(List<Message> messages) {
            this.messageList.addAll(messages);
            return this;
        }

        public MessageList build() {
            return new MessageList(this);
        }
    }
}
