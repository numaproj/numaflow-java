package io.numaproj.numaflow.reducestreamer;

import io.numaproj.numaflow.reducestreamer.model.IntervalWindow;
import io.numaproj.numaflow.reducestreamer.model.Metadata;
import lombok.AllArgsConstructor;

@AllArgsConstructor
class MetadataImpl implements Metadata {
    private final IntervalWindow intervalWindow;

    @Override
    public IntervalWindow getIntervalWindow() {
        return intervalWindow;
    }
}
