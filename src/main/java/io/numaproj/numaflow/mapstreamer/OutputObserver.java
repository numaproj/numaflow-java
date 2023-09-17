package io.numaproj.numaflow.mapstreamer;


/**
 * OutputObserver receives messages from the MapStreamer.
 */
public interface OutputObserver {
    /**
     * method will be used for sending messages to the output.
     * @param message the message to be sent
     */
    void send(Message message);
}
