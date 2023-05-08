package io.numaproj.numaflow.examples.function.map.flatmap;

import io.numaproj.numaflow.function.FunctionServer;
import io.numaproj.numaflow.function.handlers.MapHandler;
import io.numaproj.numaflow.function.interfaces.Datum;
import io.numaproj.numaflow.function.types.Message;
import io.numaproj.numaflow.function.types.MessageList;

/**
 * This is a simple User Defined Function example which processes the input message
 * and produces more than one output messages(flatMap)
 * example : if the input message is "dog,cat", it produces two output messages
 * "dog" and "cat"
 */

public class FlatMapFunction extends MapHandler {

    public static void main(String[] args) throws Exception {
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
