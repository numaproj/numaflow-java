package io.numaproj.numaflow.reducestreamer.model;

import lombok.AllArgsConstructor;

/**
 * MetadataImpl implements Metadata interface which will be passed to reduce handlers
 */
@AllArgsConstructor
public class MetadataImpl implements Metadata {
    private final IntervalWindow intervalWindow;

    @Override
    public IntervalWindow getIntervalWindow() {
        return intervalWindow;
    }
}
