package io.numaproj.numaflow.function;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

import java.util.List;

/**
 * MessageTList is used to return the list of MessageT from UDF
 */

@Getter
@Builder(builderMethodName = "newBuilder")
public class MessageTList {

    @Singular("addMessage")
    private List<MessageT> messages;

    public static class MessageTListBuilder {
        public MessageTListBuilder addAllMessages(List<MessageT> messages) {
            this.messages.addAll(messages);
            return this;
        }
    }
}
