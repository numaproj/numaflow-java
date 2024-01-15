package io.numaproj.numaflow.reducestreamer.user;

import io.numaproj.numaflow.reducestreamer.model.Message;

/**
 * OutputObserver receives messages from the MapStreamer.
 */
public interface OutputStreamObserver {
    /**
     * method will be used for sending messages to the output.
     *
     * @param message the message to be sent
     */
    void send(Message message);
}
