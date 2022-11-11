package io.numaproj.numaflow.function.metadata;

/**
 * MetadataImpl implements Metadata interface which will be passed to reduce handlers
 */
public class MetadataImpl implements Metadata {
    private final IntervalWindow intervalWindow;

    public MetadataImpl(IntervalWindow intervalWindow) {
        this.intervalWindow = intervalWindow;
    }

    @Override
    public IntervalWindow GetIntervalWindow() {
        return intervalWindow;
    }
}
