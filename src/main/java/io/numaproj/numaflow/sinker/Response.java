package io.numaproj.numaflow.sinker;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Response is used to send response from the user defined sinker. It contains the id of the
 * message, success status, an optional error message and a fallback status. Various static factory
 * methods are available to create a Response instance based on the processing outcome.
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Response {
  private final String id;
  private final Boolean success;
  private final String err;
  private final Boolean fallback;
  private final Boolean serve;
  private final byte[] serveResponse;
  private final Boolean onSuccess;
  private final Message onSuccessMessage;

    /**
   * Static method to create response for successful message processing.
   *
   * @param id id of the message
   * @return Response object with success status
   */
  public static Response responseOK(String id) {
    return new Response(id, true, null, false, false, null, false, null);
  }

  /**
   * Static method to create response for failed message processing.
   *
   * @param id id of the message
   * @param errMsg error message
   * @return Response object with failure status and error message
   */
  public static Response responseFailure(String id, String errMsg) {
    return new Response(id, false, errMsg, false, false, null, false, null);
  }

  /**
   * Static method to create response for fallback message. This indicates that the message should
   * be sent to the fallback sink.
   *
   * @param id id of the message
   * @return Response object with fallback status
   */
  public static Response responseFallback(String id) {
    return new Response(id, false, null, true, false, null, false, null);
  }

  /**
   * Static method to create response for serve message which is raw bytes.
   * This indicates that the message should be sent to the serving store.
   * Allows creation of serve message from raw bytes.
   *
   * @param id id of the message
   * @param serveResponse Response object to be sent to the serving store
   * @return Response object with serve status and serve response
   */
  public static Response responseServe(String id, byte[] serveResponse) {
    return new Response(id, false, null, false, true, serveResponse, false, null);
  }

  /**
   * Static method to create response for onSuccess message using the Datum object.
   * NOTE: response id is null if datum is null
   *
   * @param datum Datum object using which onSuccess message is created. Can be the original datum
   * @return Response object with onSuccess status and onSuccess message
   */
  public static Response responseOnSuccess(Datum datum) {
      if (datum == null) {
          return new Response(null, false, null, false, false, null, true, null);
      }
      return responseOnSuccess(datum.getId(), Message.fromDatum(datum));
  }

  /**
   * Overloaded static method to create response for onSuccess message. Allows creation of onSuccess message
   * from OnSuccessMessage object.
   *
   * @param id id of the message
   * @param onSuccessMessage OnSuccessMessage object to be sent to the onSuccess sink. Can be null
   * if original message needs to be written to onSuccess sink
   * @return Response object with onSuccess status and onSuccess message
   */
  public static Response responseOnSuccess(String id, Message onSuccessMessage) {
      return new Response(id, false, null, false, false, null, true, onSuccessMessage);
  }
}
