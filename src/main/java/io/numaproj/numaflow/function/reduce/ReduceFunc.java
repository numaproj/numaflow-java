package io.numaproj.numaflow.function.reduce;

import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.metadata.Metadata;


/**
 * Implementation of ReduceHandler instantiated from a function
 */
public class ReduceFunc implements ReduceHandler {

    private final GroupBy groupBy;

    public ReduceFunc(GroupBy groupBy) {
        this.groupBy = groupBy;
    }

    @Override
    public Message[] HandleDo(String key, ReduceDatumStream reduceDatumStream, Metadata md) {
//        return reduceFn.apply(key, reduceDatumStream, md);
        return null;
    }
}
