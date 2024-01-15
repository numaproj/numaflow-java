package io.numaproj.numaflow.reducestreamer.user;

import io.numaproj.numaflow.reducestreamer.model.Datum;
import io.numaproj.numaflow.reducestreamer.model.Metadata;

/**
 * TODO - add descriptions
 */
public abstract class ReduceStreamer {
    public abstract void processMessage(
            String[] keys,
            Datum datum,
            OutputStreamObserver outputStream,
            Metadata md);
}
