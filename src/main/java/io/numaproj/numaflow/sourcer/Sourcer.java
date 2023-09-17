package io.numaproj.numaflow.sourcer;

/**
 * Sourcer exposes method for reading messages from source.
 * Implementations should override the read method which will be used
 * for reading messages from source and ack method which will be used
 * for acknowledging the messages read from source and pending method
 * which will be used for getting the number of pending messages in the
 */
public abstract class Sourcer {
    /**
     * method will be used for reading messages from source.
     * @param request the request
     * @param observer the observer for the output
     */
    public abstract void read(ReadRequest request, OutputObserver observer);

    /**
     * method will be used for acknowledging messages from source.
     * @param request the request containing the offsets to be acknowledged
     */
    public abstract void ack(AckRequest request);

    /**
     * method will be used for getting the number of pending messages from source.
     * @return number of pending messages
     */
    public abstract long getPending();
}
