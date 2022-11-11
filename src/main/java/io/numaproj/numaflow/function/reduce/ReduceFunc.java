package io.numaproj.numaflow.function.reduce;

import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.utils.TriFunction;


/**
 * Implementation of ReduceHandler instantiated from a function
 */
public class ReduceFunc implements ReduceHandler {

    private final TriFunction<String, ReduceDatumStream, Metadata, Message[]> reduceFn;

    public ReduceFunc(TriFunction<String, ReduceDatumStream, Metadata, Message[]> reduceFn) {
        this.reduceFn = reduceFn;
    }

    @Override
    public Message[] HandleDo(String key, ReduceDatumStream reduceDatumStream, Metadata md) {
        return reduceFn.apply(key, reduceDatumStream, md);
    }
}
