package io.numaproj.numaflow.reducestreamer.model;

import lombok.Getter;

/** Message is used to wrap the data returned by Reduce Streamer functions. */
@Getter
public final class Message {
  private static final String[] DROP_TAGS = {"U+005C__DROP__"};
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
    // defensive copy - once the Message is created, the caller should not be able to modify it.
    this.keys = keys == null ? null : keys.clone();
    this.value = value == null ? null : value.clone();
    this.tags = tags == null ? null : tags.clone();
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
    return new Message(new byte[0], null, DROP_TAGS);
  }
}
