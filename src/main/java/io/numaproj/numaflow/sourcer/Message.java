package io.numaproj.numaflow.sourcer;

import java.time.Instant;
import java.util.Map;
import lombok.Getter;

/** Message is used to wrap the data returned by Sourcer. */
@Getter
public class Message {

  private final String[] keys;
  private final byte[] value;
  private final Offset offset;
  private final Instant eventTime;
  private final Map<String, String> headers;

  /**
   * used to create Message with value, offset and eventTime.
   *
   * @param value message value
   * @param offset message offset
   * @param eventTime message eventTime
   */
  public Message(byte[] value, Offset offset, Instant eventTime) {
    this(value, offset, eventTime, null, null);
  }

  /**
   * used to create Message with value, offset, eventTime and keys.
   *
   * @param value message value
   * @param offset message offset
   * @param eventTime message eventTime
   * @param keys message keys
   */
  public Message(byte[] value, Offset offset, Instant eventTime, String[] keys) {
    this(value, offset, eventTime, keys, null);
  }

  /**
   * used to create Message with value, offset, eventTime and headers.
   *
   * @param value message value
   * @param offset message offset
   * @param eventTime message eventTime
   * @param headers message headers
   */
  public Message(byte[] value, Offset offset, Instant eventTime, Map<String, String> headers) {
    this(value, offset, eventTime, null, headers);
  }

  /**
   * used to create Message with value, offset, eventTime, keys and headers.
   *
   * @param value message value
   * @param offset message offset
   * @param eventTime message eventTime
   * @param keys message keys
   * @param headers message headers
   */
  public Message(
      byte[] value, Offset offset, Instant eventTime, String[] keys, Map<String, String> headers) {
    // defensive copy - once the Message is created, the caller should not be able to modify it.
    this.keys = keys == null ? null : keys.clone();
    this.value = value == null ? null : value.clone();
    this.headers = headers == null ? null : Map.copyOf(headers);
    this.offset = offset == null ? null : new Offset(offset.getValue(), offset.getPartitionId());
    // The Instant class in Java is already immutable.
    this.eventTime = eventTime;
  }
}
