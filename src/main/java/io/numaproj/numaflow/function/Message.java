package io.numaproj.numaflow.function;

public class Message {
  public static final String ALL = "U+005C__ALL__";
  public static final String DROP = "U+005C__DROP__";

  private final String key;
  private final byte[] value;

  public Message(String key, byte[] value) {
    this.key = key;
    this.value = value;
  }

  // creates a Message to be dropped
  public static Message toDrop() {
    return new Message(DROP, new byte[0]);
  }

  // creates a Message that will forward to all
  public static Message toAll(byte[] value) {
    return new Message(ALL, value);
  }

  // creates a Message that will forward to specified "to"
  public static Message to(String to, byte[] value) {
    return new Message(to, value);
  }

  public String getKey() {
    return key;
  }

  public byte[] getValue() {
    return value;
  }
}
