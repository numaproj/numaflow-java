package io.numaproj.numaflow.sinker;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Response is used to send response from the user defined sinker.
 * It contains the id of the message, success status and error message.
 * If the message is processed successfully, responseOK method can be used to create the response.
 * If the message is not processed successfully, responseFailure method can be used to create the response.
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Response {
    private final String id;
    private final Boolean success;
    private final String err;

    /**
     * Static method to create response for successful message processing.
     * @param id id of the message
     * @return Response object with success status
     */
    public static Response responseOK(String id) {
        return new Response(id, true, null);
    }

    /**
     * Static method to create response for failed message processing.
     * @param id id of the message
     * @param errMsg error message
     * @return Response object with failure status and error message
     */
    public static Response responseFailure(String id, String errMsg) {
        return new Response(id, false, errMsg);
    }
}
