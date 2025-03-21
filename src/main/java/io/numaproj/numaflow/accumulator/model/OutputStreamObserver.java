package io.numaproj.numaflow.accumulator.model;

/**
 * OutputStreamObserver can be used to send the output messages to the next
 * stage of the flow inside the {@link Accumulator}.
 */
public interface OutputStreamObserver {
    /**
     * method will be used for sending messages to the output stream.
     *
     * @param message the {@link Message} to be sent
     */
    void send(Message message);
}
