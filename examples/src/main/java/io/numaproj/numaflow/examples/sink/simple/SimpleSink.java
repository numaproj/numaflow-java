package io.numaproj.numaflow.examples.sink.simple;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.numaproj.numaflow.sinker.Datum;
import io.numaproj.numaflow.sinker.DatumIterator;
import io.numaproj.numaflow.sinker.Response;
import io.numaproj.numaflow.sinker.ResponseList;
import io.numaproj.numaflow.sinker.Server;
import io.numaproj.numaflow.sinker.Sinker;
import lombok.extern.slf4j.Slf4j;


/**
 * This is a simple User Defined Sink example which logs the input message
 */

@Slf4j
public class SimpleSink extends Sinker {
    private final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        new Server(new SimpleSink()).start();
    }

    @Override
    public ResponseList processMessages(DatumIterator datumIterator) {
        ResponseList.ResponseListBuilder responseListBuilder = ResponseList.newBuilder();

        while (datumIterator.hasNext()) {
            Datum datum = datumIterator.next();
            try {
                String msg = new String(datum.getValue());
                log.info("Received message: {}", msg);
                responseListBuilder.addResponse(Response.responseOK(datum.getId()));
            } catch (Exception e) {
                responseListBuilder.addResponse(Response.responseFailure(
                        datum.getId(),
                        e.getMessage()));
            }
        }

        return responseListBuilder.build();
    }
}
