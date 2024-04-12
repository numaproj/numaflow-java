package io.numaproj.numaflow.examples.map.evenodd;

import io.numaproj.numaflow.mapper.Datum;
import io.numaproj.numaflow.mapper.GRPCConfig;
import io.numaproj.numaflow.mapper.Mapper;
import io.numaproj.numaflow.mapper.Message;
import io.numaproj.numaflow.mapper.MessageList;
import io.numaproj.numaflow.mapper.Server;
import lombok.extern.slf4j.Slf4j;

/**
 * This is a simple User Defined Function example which receives a message,
 * and attaches keys to the message based on the value, if the value is even
 * the key will be set as "even" if the value is odd the key will be set as
 * "odd"
 */

@Slf4j
public class EvenOddFunction extends Mapper {

    public static void main(String[] args) throws Exception {
        /*
        * If we want to run the server locally, we can use the GRPCConfig.newBuilder().isLocal(true).build()
        * It will start the server locally and we can send requests to the server using the MapperTestKit.Client
         */
        Server server = new Server(new EvenOddFunction());

        // Start the server
        server.start();

        // Wait for the server to shutdown
        server.awaitTermination();
    }

    public MessageList processMessage(String[] keys, Datum data) {
        int value = 0;
        try {
            value = Integer.parseInt(new String(data.getValue()));
        } catch (NumberFormatException e) {
            log.error("Error occurred while parsing int");
            return MessageList.newBuilder().addMessage(Message.toDrop()).build();
        }

        String[] outputKeys = value % 2 == 0 ? new String[]{"even"} : new String[]{"odd"};

        // tags will be used for conditional forwarding
        String[] tags = value % 2 == 0 ? new String[]{"even-tag"} : new String[]{"odd-tag"};

        return MessageList
                .newBuilder()
                .addMessage(new Message(data.getValue(), outputKeys, tags))
                .build();
    }
}
