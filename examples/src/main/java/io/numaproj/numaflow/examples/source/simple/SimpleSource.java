package io.numaproj.numaflow.examples.source.simple;

import io.numaproj.numaflow.sourcer.Server;
import io.numaproj.numaflow.sourcer.AckRequest;
import io.numaproj.numaflow.sourcer.Message;
import io.numaproj.numaflow.sourcer.Offset;
import io.numaproj.numaflow.sourcer.OutputObserver;
import io.numaproj.numaflow.sourcer.ReadRequest;
import io.numaproj.numaflow.sourcer.Sourcer;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SimpleSource is a simple implementation of Sourcer.
 * It generates messages with increasing offsets.
 * Keeps track of the offsets of the messages read and
 * acknowledges them when ack is called.
 */

public class SimpleSource extends Sourcer {
    private long readIndex = 0;
    private final Map<Long, Boolean> messages = new ConcurrentHashMap<>();

    @Override
    public void read(ReadRequest request, OutputObserver observer) {

        if (messages.entrySet().size() > 0) {
            // if there are messages not acknowledged, return
            return;
        }

        for (int i = 0; i < request.getCount(); i++) {
            // create a message with increasing offset
            Offset offset = new Offset(ByteBuffer.allocate(4).putLong(readIndex).array(), "0");
            Message message = new Message(
                    ByteBuffer.allocate(4).putLong(readIndex).array(),
                    offset,
                    Instant.now());
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
            messages.remove(ByteBuffer.wrap(offset.getValue()).getLong());
        }
    }


    @Override
    public long getPending() {
        // pending messages will be zero for a simple source
        return 0;
    }

    public static void main(String[] args) throws Exception {
        new Server(new SimpleSource()).start();
    }
}
