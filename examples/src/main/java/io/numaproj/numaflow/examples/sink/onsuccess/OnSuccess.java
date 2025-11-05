package io.numaproj.numaflow.examples.sink.onsuccess;

import io.numaproj.numaflow.examples.sink.simple.SimpleSink;
import io.numaproj.numaflow.sinker.Datum;
import io.numaproj.numaflow.sinker.DatumIterator;
import io.numaproj.numaflow.sinker.OnSuccessMessage;
import io.numaproj.numaflow.sinker.Response;
import io.numaproj.numaflow.sinker.ResponseList;
import io.numaproj.numaflow.sinker.Server;
import io.numaproj.numaflow.sinker.Sinker;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;

@Slf4j
public class OnSuccess extends Sinker {
    public static void main(String[] args) throws Exception {
        Server server = new Server(new OnSuccess());

        // Start the server
        server.start();

        // wait for the server to shut down
        server.awaitTermination();
    }

    @Override
    public ResponseList processMessages(DatumIterator datumIterator) {
        ResponseList.ResponseListBuilder responseListBuilder = ResponseList.newBuilder();
        while (true) {
            Datum datum = null;
            try {
                datum = datumIterator.next();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                continue;
            }
            // null means the iterator is closed, so we break the loop
            if (datum == null) {
                break;
            }
            try {
                String msg = new String(datum.getValue());
                log.info("Received message: {}, id: {}, headers - {}", msg, datum.getId(), datum.getHeaders());
                if (writeToPrimarySink()) {
                    log.info("Writing to onSuccess sink: {}", datum.getId());
                    responseListBuilder.addResponse(Response.responseOnSuccess(datum.getId(),
                            OnSuccessMessage.builder()
                                    .value(String.format("Successfully wrote message with ID: %s",
                                            datum.getId()).getBytes())
                                    .build()));
                } else {
                    log.info("Writing to fallback sink: {}", datum.getId());
                    responseListBuilder.addResponse(Response.responseFallback(datum.getId()));
                }
            } catch (Exception e) {
                log.warn("Error while writing to any sink: ", e);
                responseListBuilder.addResponse(Response.responseFailure(
                        datum.getId(),
                        e.getMessage()));
            }
        }
        return responseListBuilder.build();
    }

    /**
     * Example method to simulate write failures/success to primary sink.
     * Based on whether this returns true/false, we write to fallback sink / onSuccess sink
     * @return true if simulated write to primary sink is successful, false otherwise
     */
    public boolean writeToPrimarySink() {
        Random random = new Random();
        return random.nextBoolean();
    }
}
