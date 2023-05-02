package io.numaproj.numaflow.examples.function.map.forward;

import io.numaproj.numaflow.function.FunctionServer;
import io.numaproj.numaflow.function.handlers.MapHandler;
import io.numaproj.numaflow.function.interfaces.Datum;
import io.numaproj.numaflow.function.types.Message;
import io.numaproj.numaflow.function.types.MessageList;

/**
 * This is a simple User Defined Function example which forwards the message as is.
 */

public class ForwardFunction extends MapHandler {

    public static void main(String[] args) throws Exception {
        new FunctionServer().registerMapHandler(new ForwardFunction()).start();
    }

    public MessageList processMessage(String[] keys, Datum data) {
        return MessageList
                .newBuilder()
                .addMessage(new Message(data.getValue()))
                .build();
    }
}
