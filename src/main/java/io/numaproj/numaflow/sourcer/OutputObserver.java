package io.numaproj.numaflow.sourcer;

/**
 * OutputObserver receives messages from the sourcer.
 */
public interface OutputObserver {
    /**
     * method will be used for sending messages to the output.
     * @param message the message to be sent
     */
    void send(Message message);
}
