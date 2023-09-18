package io.numaproj.numaflow.sideinput;


/**
 * SideInputRetriever exposes method for retrieving side input.
 * Implementations should override the retrieveSideInput method
 * which will be used for updating the side input.
 */
public abstract class SideInputRetriever {
    /**
     * method which will be used for retrieving side input.
     * In case of failure, user can return a Message with noBroadcast flag set to true.
     * This will drop the message and will not broadcast it.
     *
     * @return Message which contains side input
     */
    public abstract Message retrieveSideInput();
}
