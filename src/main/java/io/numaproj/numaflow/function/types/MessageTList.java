package io.numaproj.numaflow.function.types;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

import java.util.ArrayList;
import java.util.Collection;


/**
 * MessageTList is used to return the list of MessageT from UDF
 */

@Getter
@Builder(builderMethodName = "newBuilder")
public class MessageTList {

    @Singular("addMessage")
    private Iterable<MessageT> messages;

    /**
     * Builder to build MessageTList
     */
    public static class MessageTListBuilder {
        /**
         *
         * @param messages to append all the messages to MessageTList
         * @return returns the builder
         */
        public MessageTListBuilder addAllMessages(Iterable<MessageT> messages) {
            if (this.messages == null) {
                this.messages = new ArrayList<>();
            }
            this.messages.addAll((Collection<? extends MessageT>) messages);
            return this;
        }
    }
}
