package io.numaproj.numaflow.examples.map.flatmap;

import io.numaproj.numaflow.mapper.Datum;
import io.numaproj.numaflow.mapper.Mapper;
import io.numaproj.numaflow.mapper.Message;
import io.numaproj.numaflow.mapper.MessageList;
import io.numaproj.numaflow.mapper.Server;

/**
 * This is a simple User Defined Function example which processes the input message
 * and produces more than one output messages(flatMap)
 * example : if the input message is "dog,cat", it produces two output messages
 * "dog" and "cat"
 */

public class FlatMapFunction extends Mapper {

    public static void main(String[] args) throws Exception {
        Server server = new Server(new FlatMapFunction());
        server.start();

        // Wait for the server to shutdown
        server.awaitTermination();
    }

    public MessageList processMessage(String[] keys, Datum data) {
        String msg = new String(data.getValue());
        String[] strs = msg.split(",");
        MessageList.MessageListBuilder listBuilder = MessageList.newBuilder();

        for (String str : strs) {
            listBuilder.addMessage(new Message(str.getBytes()));
        }

        return listBuilder.build();
    }
}
