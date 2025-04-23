package io.numaproj.numaflow.sideinput;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

/** Message is used to wrap the data returned by Side Input Retriever. */
@Getter(AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class Message {
  private final byte[] value;
  private final boolean noBroadcast;

  /**
   * createBroadcastMessage creates a new Message with the given value This is used to broadcast the
   * message to other side input vertices.
   *
   * @param value message value
   * @return returns the Message with noBroadcast flag set to false
   */
  public static Message createBroadcastMessage(byte[] value) {
    return new Message(value, false);
  }

  /**
   * createNoBroadcastMessage creates a new Message with noBroadcast flag set to true This is used
   * to drop the message and not to broadcast it to other side input vertices.
   *
   * @return returns the Message with noBroadcast flag set to true
   */
  public static Message createNoBroadcastMessage() {
    return new Message(new byte[0], true);
  }
}
