package io.numaproj.numaflow.function;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

import java.util.Collection;

/**
 * MessageTList is used to return the list of MessageT from UDF
 */

@Getter
@Builder(builderMethodName = "newBuilder")
public class MessageTList {

    @Singular("addMessage")
    private Iterable<MessageT> messages;

    public static class MessageTListBuilder {
        public MessageTListBuilder addAllMessages(Iterable<MessageT> messages) {
            this.messages.addAll((Collection<? extends MessageT>) messages);
            return this;
        }
    }
}
