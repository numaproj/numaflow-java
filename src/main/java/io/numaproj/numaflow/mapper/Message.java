package io.numaproj.numaflow.mapper;

import io.numaproj.numaflow.shared.UserMetadata;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/** Message is used to wrap the data returned by Mapper. */
@Getter
public class Message {
  private static final String[] DROP_TAGS = {"U+005C__DROP__"};
  private final String[] keys;
  private final byte[] value;
  private final String[] tags;
  private final UserMetadata userMetadata;

  /**
   * used to create Message with value, keys, tags(used for conditional forwarding) and userMetadata
   *
   * @param value message value
   * @param keys message keys
   * @param tags message tags which will be used for conditional forwarding
   * @param userMetadata user metadata, this is used to pass user defined metadata to the next vertex
   */
  public Message(byte[] value, String[] keys, String[] tags, UserMetadata userMetadata) {
    // defensive copy - once the Message is created, the caller should not be able to modify it.
    this.keys = keys == null ? null : keys.clone();
    this.value = value == null ? null : value.clone();
    this.tags = tags == null ? null : tags.clone();
    // Copy the data using copy constructor to prevent mutation
    this.userMetadata = userMetadata == null ? null : new UserMetadata(userMetadata);
  }

  /**
   * used to create Message with value.
   *
   * @param value message value
   */
  public Message(byte[] value) {
    this(value, null, null, null);
  }

  /**
   * used to create Message with value and keys.
   *
   * @param value message value
   * @param keys message keys
   */
  public Message(byte[] value, String[] keys) {
    this(value, keys, null, null);
  }

    /**
     * used to create Message with value, keys and tags(used for conditional forwarding)
     *
     * @param value message value
     * @param keys message keys
     * @param tags message tags which will be used for conditional forwarding
     */
  public Message(byte[] value, String[] keys, String[] tags) {
    this(value, keys, tags, null);
  }

  /**
   * creates a Message which will be dropped
   *
   * @return returns the Message which will be dropped
   */
  public static Message toDrop() {
    return new Message(new byte[0], null, DROP_TAGS, null);
  }
}
