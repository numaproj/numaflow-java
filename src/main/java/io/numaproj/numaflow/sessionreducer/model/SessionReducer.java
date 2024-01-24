package io.numaproj.numaflow.sessionreducer.model;

/**
 * SessionReducer exposes methods for performing session reduce operations.
 */
public abstract class SessionReducer {
    /**
     * processMessage is invoked for each session reduce input message.
     * It reads the input data from the datum and performs session reduce operations for the given keys.
     * An output stream is provided for sending back the result to the session reduce output stream.
     *
     * @param keys message keys
     * @param datum current message to be processed
     * @param outputStream observer of the reduce result, which is used to send back session reduce responses
     */
    public abstract void processMessage(
            String[] keys,
            Datum datum,
            OutputStreamObserver outputStream);

    /**
     * handleEndOfStream handles the closure of the session reduce input stream.
     * This method is invoked when the input session reduce stream is closed.
     * It provides the capability of constructing final responses based on the messages processed so far.
     *
     * @param keys message keys
     * @param outputStreamObserver observer of the reduce result, which is used to send back reduce responses
     */
    public abstract void handleEndOfStream(
            String[] keys,
            OutputStreamObserver outputStreamObserver);

    /**
     * accumulator transforms the current state of the session into a byte array representation.
     * This method is invoked when the session is to be merged with another session.
     * <p>
     * e.g. For a session reducer which tracks the number of events in a session by keeping a count integer,
     * accumulator() can represent the state of it by returning the byte array representation of the integer.
     *
     * @return the accumulator for the session reducer.
     */
    public abstract byte[] accumulator();

    /**
     * mergeAccumulator merges the current session with another session.
     * <p>
     * e.g. For session reducers which track the number of events in a session by keeping a count integer,
     * and use the byte array representation of the integer to form the accumulator,
     * mergeAccumulator() can parse the input accumulator to the corresponding count integer and add it
     * to the current session count to complete a session merge operation.
     *
     * @param accumulator the accumulator of the session to be merged in.
     */
    public abstract void mergeAccumulator(byte[] accumulator);
}
