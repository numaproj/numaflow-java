package io.numaproj.numaflow.sinker;

/**
 * Sinker exposes method for publishing messages to sink.
 * Implementations should override the processMessage method
 * which will be used for processing the input messages
 */

public abstract class Sinker {
    /**
     * method will be used for processing messages.
     * response for the message should be added to the
     * response list which will be returned by getResponse
     *
     * @param datum current message to be processed
     */
    public abstract void processMessage(Datum datum);

    /**
     * method will be used for returning the responses.
     * each message should have a response, if there are
     * n messages then there should be n responses and
     * each response should contain the id of the message
     * Response.responseOK() and Response.responseFailure() can
     * be used for creating the responses
     *
     * @return ResponseList which contains the responses
     */
    public abstract ResponseList getResponse();
}
