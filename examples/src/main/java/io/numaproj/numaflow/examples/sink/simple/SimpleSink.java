package io.numaproj.numaflow.examples.sink.simple;

import io.numaproj.numaflow.sink.Datum;
import io.numaproj.numaflow.sink.Response;
import io.numaproj.numaflow.sink.SinkDatumStream;
import io.numaproj.numaflow.sink.SinkFunc;
import io.numaproj.numaflow.sink.SinkServer;
import io.numaproj.numaflow.sink.v1.Udsink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class SimpleSink {
    private static final Logger logger = Logger.getLogger(SimpleSink.class.getName());

    private static List<Response> process(SinkDatumStream datumStream) {
        ArrayList<Response> responses = new ArrayList<>();

        while (true) {
            Datum datum = datumStream.ReadMessage();
            // EOF indicates the end of the input
            if (datum == SinkDatumStream.EOF) {
                break;
            }
            logger.info(Arrays.toString(datum.getValue()));
            responses.add(new Response(datum.getId(), true, ""));
        }
        return responses;
    }

    public static void main(String[] args) throws IOException {
        new SinkServer().registerSinker(new SinkFunc(SimpleSink::process)).start();
    }
}
