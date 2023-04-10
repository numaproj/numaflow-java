package io.numaproj.numaflow.examples.sink.simple;

import io.numaproj.numaflow.sink.Datum;
import io.numaproj.numaflow.sink.Response;
import io.numaproj.numaflow.sink.SinkDatumStream;
import io.numaproj.numaflow.sink.SinkHandler;
import io.numaproj.numaflow.sink.SinkServer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a simple User Defined Sink example which logs the input message
 */

@Slf4j
public class SimpleSink extends SinkHandler {

    public static void main(String[] args) throws IOException {
        new SinkServer().registerSinker(new SimpleSink()).start();
    }

    @Override
    public List<Response> processMessage(SinkDatumStream datumStream) {
        ArrayList<Response> responses = new ArrayList<>();

        while (true) {
            Datum datum = datumStream.ReadMessage();
            // EOF indicates the end of the input
            if (datum == SinkDatumStream.EOF) {
                break;
            }
            log.info(new String(datum.getValue()));
            responses.add(new Response(datum.getId(), true, ""));
        }
        return responses;
    }
}
