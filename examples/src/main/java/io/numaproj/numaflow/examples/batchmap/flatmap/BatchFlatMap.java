package io.numaproj.numaflow.examples.batchmap.flatmap;

import io.numaproj.numaflow.batchmapper.BatchMapper;
import io.numaproj.numaflow.batchmapper.BatchResponse;
import io.numaproj.numaflow.batchmapper.BatchResponses;
import io.numaproj.numaflow.batchmapper.Datum;
import io.numaproj.numaflow.batchmapper.DatumIterator;
import io.numaproj.numaflow.batchmapper.Message;
import io.numaproj.numaflow.batchmapper.Server;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BatchFlatMap extends BatchMapper {
    @Override
    public BatchResponses processMessage(DatumIterator datumStream) {
        BatchResponses batchResponses = new BatchResponses();
        while (true) {
            Datum datum = null;
            try {
                datum = datumStream.next();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                continue;
            }
            // null means the iterator is closed so we are good to break the loop.
            if (datum == null) {
                break;
            }
            try {
                String msg = new String(datum.getValue());
                String[] strs = msg.split(",");
                BatchResponse batchResponse = new BatchResponse(datum.getId());
                for (String str : strs) {
                    batchResponse.append(new Message(str.getBytes()));
                }
                batchResponses.append(batchResponse);
            } catch (Exception e) {
                batchResponses.append(new BatchResponse(datum.getId()));
            }
        }
        return batchResponses;
    }

    public static void main(String[] args) throws Exception {
        Server server = new Server(new BatchFlatMap());

        // Start the server
        server.start();

        // wait for the server to shutdown
        server.awaitTermination();
    }
}
