package io.numaproj.numaflow.sourcetransformer;


/**
 * SourceTransformer exposes method for performing transform operation
 * in the source. Implementations should override the processMessage
 * method which will be used for transforming and assigning event time
 * to input messages
 */

public abstract class SourceTransformer {
    /**
     * method which will be used for processing messages.
     *
     * @param keys message keys
     * @param datum current message to be processed
     *
     * @return MessageList which contains output from the transform function
     */
    public abstract MessageList processMessage(String[] keys, Datum datum);

}
