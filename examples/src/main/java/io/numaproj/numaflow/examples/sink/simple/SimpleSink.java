package io.numaproj.numaflow.examples.sink.simple;

import io.numaproj.numaflow.sink.Response;
import io.numaproj.numaflow.sink.SinkFunc;
import io.numaproj.numaflow.sink.SinkServer;
import io.numaproj.numaflow.sink.v1.Udsink;

import java.io.IOException;
import java.util.logging.Logger;

public class SimpleSink {
    private static final Logger logger = Logger.getLogger(SimpleSink.class.getName());

    private static Response[] process(Udsink.Datum[] datumList) {
        Response[] responses = new Response[datumList.length];
        for (int i = 0; i < datumList.length; i++) {
            logger.info(datumList[i].getValue().toStringUtf8());
            responses[i] = new Response(datumList[i].getId(), true, "");
        }
        return responses;
    }

    public static void main(String[] args) throws IOException {
        new SinkServer().registerSinker(new SinkFunc(SimpleSink::process)).start();
    }
}
