package io.numaproj.numaflow.sink;

import java.util.List;
import java.util.function.Function;

/**
 * Implementation of SinkHandler instantiated from a function
 */
public class SinkFunc implements SinkHandler {

    private final Function<SinkDatumStream, List<Response>> sinkFn;

    public SinkFunc(Function<SinkDatumStream, List<Response>> sinkFn) {
        this.sinkFn = sinkFn;
    }

    @Override
    public List<Response> HandleDo(SinkDatumStream datumStream) {
        return sinkFn.apply(datumStream);
    }
}
