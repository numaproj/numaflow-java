package io.numaproj.numaflow.examples.sink.simple;

import io.numaproj.numaflow.sinker.Datum;
import io.numaproj.numaflow.sinker.Response;
import io.numaproj.numaflow.sinker.Server;
import io.numaproj.numaflow.sinker.Sinker;
import lombok.extern.slf4j.Slf4j;

/**
 * This is a simple User Defined Sink example which logs the input message
 */

@Slf4j
public class SimpleSink extends Sinker {

    public static void main(String[] args) throws Exception {
        new Server(new SimpleSink()).start();
    }

    @Override
    public Response processMessage(Datum datum) {
        log.info(new String(datum.getValue()));
        return Response.responseOK(datum.getId());
    }
}
