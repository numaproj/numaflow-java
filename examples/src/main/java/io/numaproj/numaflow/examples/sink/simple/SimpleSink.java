package io.numaproj.numaflow.examples.sink.simple;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.numaproj.numaflow.sinker.Datum;
import io.numaproj.numaflow.sinker.Response;
import io.numaproj.numaflow.sinker.ResponseList;
import io.numaproj.numaflow.sinker.Server;
import io.numaproj.numaflow.sinker.Sinker;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

/**
 * This is a simple User Defined Sink example which logs the input message
 */

@Slf4j
public class SimpleSink extends Sinker {
    private final ObjectMapper mapper = new ObjectMapper();
    private final ResponseList.ResponseListBuilder responseListBuilder = ResponseList.newBuilder();
    public static void main(String[] args) throws Exception {
        new Server(new SimpleSink()).start();
    }

    @Override
    public void processMessage(Datum datum) {
        try {
            String decodedMessage = mapper.readValue(datum.getValue(), String.class);
            log.info("Decoded message - {}", decodedMessage);
            responseListBuilder.addResponse(Response.responseOK(datum.getId()));
        } catch (IOException e) {
            responseListBuilder.addResponse(Response.responseFailure(datum.getId(), e.getMessage()));
        }
    }

    @Override
    public ResponseList getResponse() {
        // Reset the builder after building the response to avoid keeping old responses in memory
        // this is required as the same sinker instance is used for multiple requests
        try {
            return responseListBuilder.build();
        } finally {
            responseListBuilder.clearResponses();
        }
    }
}
