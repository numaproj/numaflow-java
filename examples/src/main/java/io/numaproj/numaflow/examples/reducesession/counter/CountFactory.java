package io.numaproj.numaflow.examples.reducesession.counter;

import io.numaproj.numaflow.sessionreducer.Server;
import io.numaproj.numaflow.sessionreducer.model.SessionReducerFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * CountFactory extends SessionReducerFactory to support creating instances of SumFunction.
 * It also provides a main function to start a server for handling the session reduce stream.
 */
@Slf4j
public class CountFactory extends SessionReducerFactory<CountFunction> {

    public static void main(String[] args) throws Exception {
        log.info("count udf was invoked");
        Server server = new Server(new CountFactory());

        // Start the server
        server.start();

        // wait for the server to shut down
        server.awaitTermination();
    }

    @Override
    public CountFunction createSessionReducer() {
        return new CountFunction();
    }
}
