package io.numaproj.numaflow.examples.servingstore.memory;

import io.numaproj.numaflow.servingstore.GetDatum;
import io.numaproj.numaflow.servingstore.Payload;
import io.numaproj.numaflow.servingstore.PutDatum;
import io.numaproj.numaflow.servingstore.Server;
import io.numaproj.numaflow.servingstore.ServingStorer;
import io.numaproj.numaflow.servingstore.StoredResult;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ServingInMemoryStore extends ServingStorer {
    private final Map<String, List<Payload>> store = new HashMap<>();

    public void put(PutDatum putDatum) {
        log.info("Putting data into the store with ID: {}", putDatum.ID());
        store.put(putDatum.ID(), putDatum.Payloads());
    }

    public StoredResult get(GetDatum getDatum) {
        log.info("Getting data from the store with ID: {}", getDatum.ID());
        List<Payload> payloads = store.getOrDefault(getDatum.ID(), Collections.emptyList());
        return new StoredResult(getDatum.ID(), payloads);
    }

    public static void main(String[] args) throws Exception {
        Server server = new Server(new ServingInMemoryStore());

        // Start the server
        server.start();

        // Wait for the server to shutdown
        server.awaitTermination();
    }
}
