package io.numaproj.numaflow.examples.function.reduce.sum;

import io.numaproj.numaflow.reducer.ReducerFactory;
import io.numaproj.numaflow.reducer.Server;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SumFactory extends ReducerFactory<SumFunction> {

    public static void main(String[] args) throws Exception {
        log.info("sum udf was invoked");
        new Server(new SumFactory()).start();
    }

    @Override
    public SumFunction createReducer() {
        return new SumFunction();
    }
}
