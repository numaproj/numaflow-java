package io.numaproj.numaflow.examples.reducestreamer.sum;

import io.numaproj.numaflow.reducestreamer.Server;
import io.numaproj.numaflow.reducestreamer.model.ReduceStreamerFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SumFactory extends ReduceStreamerFactory<SumFunction> {

    public static void main(String[] args) throws Exception {
        log.info("sum udf was invoked");
        new Server(new SumFactory()).start();
    }

    @Override
    public SumFunction createReduceStreamer() {
        return new SumFunction();
    }
}
