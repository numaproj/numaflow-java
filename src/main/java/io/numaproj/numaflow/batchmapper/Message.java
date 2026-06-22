package io.numaproj.numaflow.batchmapper;

import io.numaproj.numaflow.shared.NackOptions;
import lombok.Getter;

/** Message is used to wrap the data returned by Mapper. */
@Getter
public class Message {
  private static final String[] DROP_TAGS = {"U+005C__DROP__"};
  private static final String[] NACK_TAGS = {"U+005C__NACK__"};
  private final String[] keys;
  private final byte[] value;
  private final String[] tags;
  private final NackOptions nackOptions;

  /**
   * used to create Message with value, keys and tags(used for conditional forwarding)
   *
   * @param value message value
   * @param keys message keys
   * @param tags message tags which will be used for conditional forwarding
   */
  public Message(byte[] value, String[] keys, String[] tags) {
    this(value, keys, tags, (NackOptions) null);
  }

  private Message(byte[] value, String[] keys, String[] tags, NackOptions nackOptions) {
    // defensive copy - once the Message is created, the caller should not be able to modify it.
    this.keys = keys == null ? null : keys.clone();
    this.value = value == null ? null : value.clone();
    this.tags = tags == null ? null : tags.clone();
    this.nackOptions = nackOptions;
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

  /**
   * creates a Message that negatively acknowledges the input message, requesting redelivery.
   *
   * @param nackOptions optional redelivery options (may be null)
   * @return the Message which will be nacked
   */
  public static Message toNack(NackOptions nackOptions) {
    return new Message(new byte[0], null, NACK_TAGS, nackOptions);
  }
}
