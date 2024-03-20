package io.numaproj.numaflow.sessionreducer.model;

/**
 * OutputStreamObserver sends to the output stream, the messages generate by the session reducer.
 */
public interface OutputStreamObserver {
    /**
     * method will be used for sending messages to the output stream.
     *
     * @param message the message to be sent
     */
    void send(Message message);
}
