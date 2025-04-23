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
public final class Response {
    private final String id;
    private final boolean success;
    private final String err;
    private final boolean fallback;

    /**
     * Static method to create response for successful message processing.
     *
     * @param id id of the message
     *
     * @return Response object with success status
     */
    public static Response responseOK(String id) {
        return new Response(id, true, null, false);
    }

    /**
     * Static method to create response for failed message processing.
     *
     * @param id id of the message
     * @param errMsg error message
     *
     * @return Response object with failure status and error message
     */
    public static Response responseFailure(String id, String errMsg) {
        return new Response(id, false, errMsg, false);
    }


    /**
     * Static method to create response for fallback message.
     * This indicates that the message should be sent to the fallback sink.
     *
     * @param id id of the message
     *
     * @return Response object with fallback status
     */
    public static Response responseFallback(String id) {
        return new Response(id, false, null, true);
    }
}
