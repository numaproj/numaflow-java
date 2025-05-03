package io.numaproj.numaflow.accumulator.model;

import java.time.Instant;
import java.util.Map;
import lombok.Getter;

/** Message is used to wrap the data returned by Accumulator functions. */
@Getter
public class Message {
  private final Instant eventTime;
  private final Instant watermark;
  private final Map<String, String> headers;
  private final String id;
  private String[] keys;
  private byte[] value;
  private String[] tags;

  /**
   * Constructor for constructing message from Datum, it is advised to use the incoming Datum to
   * construct the message, because event time, watermark, id and headers of the message are derived
   * from the Datum. Only use custom implementation of the Datum if you know what you are doing.
   *
   * @param datum {@link Datum} object
   */
  public Message(Datum datum) {
    this.keys = datum.getKeys();
    this.value = datum.getValue();
    this.headers = datum.getHeaders();
    this.eventTime = datum.getEventTime();
    this.watermark = datum.getWatermark();
    this.id = datum.getID();
    this.tags = null;
  }

  /*
   * sets the value of the message
   *
   * @param value byte array of the value
   */
  public void setValue(byte[] value) {
    this.value = value;
  }

  /*
   * sets the keys of the message
   *
   * @param keys string array of the keys
   */
  public void setKeys(String[] keys) {
    this.keys = keys;
  }

  /*
   * sets the tags of the message, tags are used for conditional forwarding
   *
   * @param tags string array of the tags
   */
  public void setTags(String[] tags) {
    this.tags = tags;
  }
}
