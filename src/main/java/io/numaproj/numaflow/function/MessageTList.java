package io.numaproj.numaflow.function;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * MessageTList is used to return the list of MessageT from UDF
 */

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MessageTList {
    private List<MessageT> messageTList;

    private MessageTList(MessageTListBuilder messageTListBuilder) {
        this.messageTList = messageTListBuilder.messageList;
    }

    public static MessageTListBuilder builder() {
        return new MessageTListBuilder();
    }

    public List<MessageT> getMessages() {
        return messageTList;
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class MessageTListBuilder {
        private final List<MessageT> messageList = new ArrayList<>();

        public MessageTListBuilder addMessage(MessageT message) {
            this.messageList.add(message);
            return this;
        }

        public MessageTListBuilder addMessages(List<MessageT> messages) {
            this.messageList.addAll(messages);
            return this;
        }

        public MessageTList build() {
            return new MessageTList(this);
        }
    }
}
