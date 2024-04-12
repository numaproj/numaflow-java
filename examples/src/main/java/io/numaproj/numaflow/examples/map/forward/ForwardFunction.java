package io.numaproj.numaflow.examples.map.forward;

import io.numaproj.numaflow.mapper.Datum;
import io.numaproj.numaflow.mapper.Mapper;
import io.numaproj.numaflow.mapper.Message;
import io.numaproj.numaflow.mapper.MessageList;
import io.numaproj.numaflow.mapper.Server;

/**
 * This is a simple User Defined Function example which forwards the message as is.
 */

public class ForwardFunction extends Mapper {
    public static void main(String[] args) throws Exception {
        Server server = new Server(new ForwardFunction());
        server.start();

        // Wait for the server to shutdown
        server.awaitTermination();
    }

    public MessageList processMessage(String[] keys, Datum data) {
        return MessageList
                .newBuilder()
                .addMessage(new Message(data.getValue(), keys))
                .build();
    }
}
