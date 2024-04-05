package io.numaproj.numaflow.examples.source.simple;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Longs;
import io.numaproj.numaflow.sourcer.AckRequest;
import io.numaproj.numaflow.sourcer.Message;
import io.numaproj.numaflow.sourcer.Offset;
import io.numaproj.numaflow.sourcer.OutputObserver;
import io.numaproj.numaflow.sourcer.ReadRequest;
import io.numaproj.numaflow.sourcer.Server;
import io.numaproj.numaflow.sourcer.Sourcer;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SimpleSource is a simple implementation of Sourcer.
 * It generates messages with increasing offsets.
 * Keeps track of the offsets of the messages read and
 * acknowledges them when ack is called.
 */

public class SimpleSource extends Sourcer {
    private final Map<Long, Boolean> messages = new ConcurrentHashMap<>();
    private long readIndex = 0;

    public static void main(String[] args) throws Exception {
        new Server(new SimpleSource()).start();
    }

    @Override
    public void read(ReadRequest request, OutputObserver observer) {
        long startTime = System.currentTimeMillis();
        if (messages.entrySet().size() > 0) {
            // if there are messages not acknowledged, return
            return;
        }

        for (int i = 0; i < request.getCount(); i++) {
            if (System.currentTimeMillis() - startTime > request.getTimeout().toMillis()) {
                return;
            }

            Map<String, String> headers = new HashMap<>();
            headers.put("x-txn-id", UUID.randomUUID().toString());

            // create a message with increasing offset
            Offset offset = new Offset(Longs.toByteArray(readIndex));
            Message message = new Message(
                    Long.toString(readIndex).getBytes(),
                    offset,
                    Instant.now(),
                    headers);
            // send the message to the observer
            observer.send(message);
            // keep track of the messages read and not acknowledged
            messages.put(readIndex, true);
            readIndex += 1;
        }
    }

    @Override
    public void ack(AckRequest request) {
        // remove the acknowledged messages from the map
        for (Offset offset : request.getOffsets()) {
            messages.remove(Longs.fromByteArray(offset.getValue()));
        }
    }

    @Override
    public long getPending() {
        // pending messages will be zero for a simple source
        return 0;
    }

    @Override
    public List<Integer> getPartitions() {
        return Sourcer.defaultPartitions();
    }
}
