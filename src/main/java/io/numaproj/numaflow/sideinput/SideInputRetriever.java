package io.numaproj.numaflow.sideinput;


/**
 * SideInputRetriever exposes method for retrieving side input.
 * Implementations should override the retrieveSideInput method
 * which will be used for updating the side input.
 */
public abstract class SideInputRetriever {
    public abstract Message retrieveSideInput();
}
