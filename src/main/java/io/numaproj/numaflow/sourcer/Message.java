package io.numaproj.numaflow.sourcer;

import java.time.Instant;
import java.util.Map;

import io.numaproj.numaflow.shared.UserMetadata;
import lombok.Getter;

/** Message is used to wrap the data returned by Sourcer. */
@Getter
public class Message {

  private final String[] keys;
  private final byte[] value;
  private final Offset offset;
  private final Instant eventTime;
  private final Map<String, String> headers;
  private final UserMetadata userMetadata;

  /**
   * used to create Message with value, offset and eventTime.
   *
   * @param value message value
   * @param offset message offset
   * @param eventTime message eventTime
   */
  public Message(byte[] value, Offset offset, Instant eventTime) {
    this(value, offset, eventTime, null, null, null);
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
    this(value, offset, eventTime, keys, null, null);
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
    this(value, offset, eventTime, null, headers, null);
  }

  /**
   * used to create Message with value, offset, eventTime and userMetadata.
   *
   * @param value message value
   * @param offset message offset
   * @param eventTime message eventTime
   * @param userMetadata message userMetadata
   */
  public Message(byte[] value, Offset offset, Instant eventTime, UserMetadata userMetadata) {
    this(value, offset, eventTime, null, null, userMetadata);
  }

  /**
   * used to create Message with value, offset, eventTime, keys and userMetadata.
   *
   * @param value message value
   * @param offset message offset
   * @param eventTime message eventTime
   * @param keys message keys
   * @param userMetadata message userMetadata
   */
  public Message(byte[] value, Offset offset, Instant eventTime, String[] keys, UserMetadata userMetadata) {
    this(value, offset, eventTime, keys, null, userMetadata);
  }

  /**
   * used to create Message with value, offset, eventTime, headers and userMetadata.
   *
   * @param value message value
   * @param offset message offset
   * @param eventTime message eventTime
   * @param headers message headers
   * @param userMetadata message userMetadata
   */
  public Message(byte[] value, Offset offset, Instant eventTime, Map<String, String> headers, UserMetadata userMetadata) {
    this(value, offset, eventTime, null, headers, userMetadata);
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
  public Message(byte[] value, Offset offset, Instant eventTime, String[] keys, Map<String, String> headers) {
    this(value, offset, eventTime, keys, headers, null);
  }

  /**
   * used to create Message with value, offset, eventTime, keys, headers and userMetadata.
   *
   * @param value message value
   * @param offset message offset
   * @param eventTime message eventTime
   * @param keys message keys
   * @param headers message headers
   * @param userMetadata message userMetadata
   */
  public Message(
      byte[] value, Offset offset, Instant eventTime, String[] keys, Map<String, String> headers, UserMetadata userMetadata) {
    // defensive copy - once the Message is created, the caller should not be able to modify it.
    this.keys = keys == null ? null : keys.clone();
    this.value = value == null ? null : value.clone();
    this.headers = headers == null ? null : Map.copyOf(headers);
    this.offset = offset == null ? null : new Offset(offset.getValue(), offset.getPartitionId());
    // The Instant class in Java is already immutable.
    this.eventTime = eventTime;
    // Copy the data using copy constructor to prevent mutation
    this.userMetadata = userMetadata == null ? null : new UserMetadata(userMetadata);
  }
}
