package io.numaproj.numaflow.examples.reduce.sum;

import io.numaproj.numaflow.reducer.ReducerFactory;
import io.numaproj.numaflow.reducer.Server;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SumFactory extends ReducerFactory<SumFunction> {

    public static void main(String[] args) throws Exception {
        log.info("Starting sum udf server");
        Server server = new Server(new SumFactory());

        // Start the server
        server.start();

        // wait for the server to shut down
        server.awaitTermination();
    }

    @Override
    public SumFunction createReducer() {
        return new SumFunction();
    }
}
