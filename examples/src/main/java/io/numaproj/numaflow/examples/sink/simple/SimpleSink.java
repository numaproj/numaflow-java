package io.numaproj.numaflow.examples.sink.simple;

import io.numaproj.numaflow.sink.SinkServer;
import io.numaproj.numaflow.sink.handler.SinkHandler;
import io.numaproj.numaflow.sink.interfaces.Datum;
import io.numaproj.numaflow.sink.types.Response;
import lombok.extern.slf4j.Slf4j;

/**
 * This is a simple User Defined Sink example which logs the input message
 */

@Slf4j
public class SimpleSink extends SinkHandler {

    public static void main(String[] args) throws Exception {
        new SinkServer().registerSinker(new SimpleSink()).start();
    }

    @Override
    public Response processMessage(Datum datum) {
        log.info(new String(datum.getValue()));
        return Response.responseOK(datum.getId());
    }
}
