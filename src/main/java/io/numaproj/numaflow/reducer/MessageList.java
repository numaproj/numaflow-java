package io.numaproj.numaflow.reducer;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

import java.util.ArrayList;
import java.util.Collection;

/**
 * MessageList is used to return the list of Messages returned from Reducer functions.
 */

@Getter
@Builder(builderMethodName = "newBuilder")
public class MessageList {

    @Singular("addMessage")
    private Iterable<Message> messages;

    /**
     * Builder to build MessageList
     */
    public static class MessageListBuilder {
        /**
         *
         * @param messages to append all the messages to MessageList
         * @return returns the builder
         */
        public MessageListBuilder addMessages(Iterable<Message> messages) {
            if (this.messages == null) {
                this.messages = new ArrayList<>();
                return this;
            }
            this.messages.addAll((Collection<? extends Message>) messages);
            return this;
        }
    }
}
