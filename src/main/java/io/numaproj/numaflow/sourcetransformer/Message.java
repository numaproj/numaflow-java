package io.numaproj.numaflow.sourcetransformer;

import java.time.Instant;
import lombok.Getter;

/** Message is used to wrap the data return by SourceTransformer functions. */
@Getter
public class Message {
  private static final String[] DROP_TAGS = {"U+005C__DROP__"};
  private final String[] keys;
  private final byte[] value;
  private final Instant eventTime;
  private final String[] tags;

  /**
   * used to create Message with value, eventTime, keys and tags(used for conditional forwarding)
   *
   * @param value message value
   * @param eventTime message eventTime
   * @param keys message keys
   * @param tags message tags which will be used for conditional forwarding
   */
  public Message(byte[] value, Instant eventTime, String[] keys, String[] tags) {
    // defensive copy - once the Message is created, the caller should not be able to modify it.
    this.keys = keys == null ? null : keys.clone();
    this.value = value == null ? null : value.clone();
    this.tags = tags == null ? null : tags.clone();
    // The Instant class in Java is already immutable.
    this.eventTime = eventTime;
  }

  /**
   * used to create Message with value and eventTime.
   *
   * @param value message value
   * @param eventTime message eventTime
   */
  public Message(byte[] value, Instant eventTime) {
    this(value, eventTime, null, null);
  }

  /**
   * used to create Message with value, eventTime and keys
   *
   * @param value message value
   * @param eventTime message eventTime
   * @param keys message keys
   */
  public Message(byte[] value, Instant eventTime, String[] keys) {
    this(value, eventTime, keys, null);
  }

  /**
   * creates a Message which will be dropped.
   *
   * @param eventTime message eventTime is required because even though a message is dropped, we
   *     consider it as being processed, hence it should be counted in the watermark calculation
   *     using the provided event time.
   * @return returns the Message which will be dropped
   */
  public static Message toDrop(Instant eventTime) {
    return new Message(new byte[0], eventTime, null, DROP_TAGS);
  }
}
