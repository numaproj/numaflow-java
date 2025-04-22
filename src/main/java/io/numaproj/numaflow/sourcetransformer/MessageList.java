package io.numaproj.numaflow.sourcetransformer;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.AccessLevel;

import java.util.ArrayList;
import java.util.List;

/** MessageList is used to return the list of Messages from SourceTransformer functions. */
@Getter(AccessLevel.PROTECTED)
@Builder(builderMethodName = "newBuilder")
public class MessageList {

  @Singular("addMessage")
  private List<Message> messages;

  /** Builder to build MessageList */
  public static class MessageListBuilder {
    /**
     * @param messages to append all the messages to MessageList
     * @return returns the builder
     */
    public MessageListBuilder addAllMessages(Iterable<Message> messages) {
      if (this.messages == null) {
        this.messages = new ArrayList<>();
      }

      for (Message message : messages) {
        this.messages.add(message);
      }
      return this;
    }
  }
}
