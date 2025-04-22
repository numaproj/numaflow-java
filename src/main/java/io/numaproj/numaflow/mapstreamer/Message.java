package io.numaproj.numaflow.mapstreamer;

import lombok.Getter;
import lombok.AccessLevel;

/** Message is used to wrap the data returned by MapStreamer. */
@Getter(AccessLevel.PROTECTED)
public class Message {
  public static final String DROP = "U+005C__DROP__";

  private final String[] keys;
  private final byte[] value;
  private final String[] tags;

  /**
   * used to create Message with value, keys and tags(used for conditional forwarding)
   *
   * @param value message value
   * @param keys message keys
   * @param tags message tags which will be used for conditional forwarding
   */
  public Message(byte[] value, String[] keys, String[] tags) {
    this.keys = keys;
    this.value = value;
    this.tags = tags;
  }

  /**
   * used to create Message with value.
   *
   * @param value message value
   */
  public Message(byte[] value) {
    this(value, null, null);
  }

  /**
   * used to create Message with value and keys.
   *
   * @param value message value
   * @param keys message keys
   */
  public Message(byte[] value, String[] keys) {
    this(value, keys, null);
  }

  /**
   * creates a Message which will be dropped
   *
   * @return returns the Message which will be dropped
   */
  public static Message toDrop() {
    return new Message(new byte[0], null, new String[] {DROP});
  }
}
