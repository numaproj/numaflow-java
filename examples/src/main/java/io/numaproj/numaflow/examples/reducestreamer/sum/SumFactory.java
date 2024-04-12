package io.numaproj.numaflow.examples.reducestreamer.sum;

import io.numaproj.numaflow.reducestreamer.Server;
import io.numaproj.numaflow.reducestreamer.model.ReduceStreamerFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * SumFactory extends ReduceStreamerFactory to support creating instances of SumFunction.
 * It also provides a main function to start a server for handling the reduce stream.
 */
@Slf4j
public class SumFactory extends ReduceStreamerFactory<SumFunction> {

    public static void main(String[] args) throws Exception {
        log.info("sum udf was invoked");
        Server server = new Server(new SumFactory());

        // Start the server
        server.start();

        // wait for the server to shut down
        server.awaitTermination();
    }

    @Override
    public SumFunction createReduceStreamer() {
        return new SumFunction();
    }
}
