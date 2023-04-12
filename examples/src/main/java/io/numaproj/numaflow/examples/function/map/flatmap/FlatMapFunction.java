package io.numaproj.numaflow.examples.function.map.flatmap;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.FunctionServer;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.MessageList;
import io.numaproj.numaflow.function.map.MapHandler;

import java.io.IOException;

/**
 * This is a simple User Defined Function example which processes the input message
 * and produces more than one output messages(flatMap)
 * example : if the input message is "dog,cat", it produces two output messages
 * "dog" and "cat"
 */

public class FlatMapFunction extends MapHandler {

    public static void main(String[] args) throws IOException {
        new FunctionServer().registerMapHandler(new FlatMapFunction()).start();
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
