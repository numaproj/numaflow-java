package io.numaproj.numaflow.reducer;

import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

@Getter(AccessLevel.PROTECTED)
@Builder(builderMethodName = "newBuilder")
public final class MessageList {

  @Singular("addMessage")
  private List<Message> messages;

  /** Builder to build MessageList */
  public static class MessageListBuilder {
    /**
     * @param messages to append all the messages to MessageList
     * @return returns the builder
     */
    public MessageListBuilder addMessages(Iterable<Message> messages) {
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
