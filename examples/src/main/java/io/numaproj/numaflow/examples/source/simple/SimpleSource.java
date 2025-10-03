package io.numaproj.numaflow.examples.source.simple;

import io.numaproj.numaflow.sourcer.AckRequest;
import io.numaproj.numaflow.sourcer.Message;
import io.numaproj.numaflow.sourcer.NackRequest;
import io.numaproj.numaflow.sourcer.Offset;
import io.numaproj.numaflow.sourcer.OutputObserver;
import io.numaproj.numaflow.sourcer.ReadRequest;
import io.numaproj.numaflow.sourcer.Server;
import io.numaproj.numaflow.sourcer.Sourcer;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SimpleSource is a simple implementation of Sourcer.
 * It generates messages with increasing offsets.
 * Keeps track of the offsets of the messages read and
 * acknowledges them when ack is called.
 */

@Slf4j
public class SimpleSource extends Sourcer {
    private final Map<Integer, Boolean> yetToBeAcked = new ConcurrentHashMap<>();
    Map<Integer, Boolean> nacked = new ConcurrentHashMap<>();
    private final AtomicInteger readIndex = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        Server server = new Server(new SimpleSource());

        // Start the server
        server.start();

        // wait for the server to shut down
        server.awaitTermination();
    }

    @Override
    public void read(ReadRequest request, OutputObserver observer) {
        long startTime = System.currentTimeMillis();

        // if there are messages which got nacked, we should read them first.
        if (!nacked.isEmpty()) {
            for (int i = 0; i < nacked.size(); i++) {
                Integer index = readIndex.incrementAndGet();
                yetToBeAcked.put(index, true);
                observer.send(constructMessage(index));
            }
            nacked.clear();
        }

        if (!yetToBeAcked.isEmpty()) {
            // if there are messages not acknowledged, return
            return;
        }

        Integer index = readIndex.incrementAndGet();

        for (int i = 0; i < request.getCount(); i++) {
            if (System.currentTimeMillis() - startTime > request.getTimeout().toMillis()) {
                return;
            }
            // send the message to the observer
            observer.send(constructMessage(index));
            // keep track of the messages read and not acknowledged
            yetToBeAcked.put(index, true);
        }
    }

    @Override
    public void ack(AckRequest request) {
        for (Offset offset : request.getOffsets()) {
            Integer decoded_offset = ByteBuffer.wrap(offset.getValue()).getInt();
            // remove the acknowledged messages from the map
            yetToBeAcked.remove(decoded_offset);
        }
    }

    @Override
    public void nack(NackRequest request) {
        // put them to nacked offsets so that they will be retried immediately.
        for (Offset offset : request.getOffsets()) {
            Integer decoded_offset = ByteBuffer.wrap(offset.getValue()).getInt();
            yetToBeAcked.remove(decoded_offset);
            nacked.put(decoded_offset, true);
            readIndex.decrementAndGet();
        }
    }

    @Override
    public long getPending() {
        // number of messages not acknowledged yet
        return yetToBeAcked.size();
    }

    @Override
    public List<Integer> getPartitions() {
        return Sourcer.defaultPartitions();
    }

    private Message constructMessage(Integer readIndex) {
        Map<String, String> headers = new HashMap<>();
        headers.put("x-txn-id", UUID.randomUUID().toString());

        // create a message with increasing offset
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(readIndex);
        Offset offset = new Offset(buffer.array());
        return new Message(
                Integer.toString(readIndex).getBytes(),
                offset,
                Instant.now(),
                headers);
    }
}
