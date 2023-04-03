package io.numaproj.numaflow.examples.function.map.flatmap;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.FunctionServer;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.map.MapHandler;

import java.io.IOException;

/**
 * This is a simple User Defined Function example which processes the input message
 * and produces more than one output messages(flatMap)
 * example : if the input message is "dog,cat", it produces two output messages
 * "dog" and "cat"
 */

public class FlatMapFunction extends MapHandler {

    public Message[] processMessage(String[] key, Datum data) {
        String msg = new String(data.getValue());
        String[] strs = msg.split(",");
        Message[] results = new Message[strs.length];

        for (int i = 0; i < strs.length; i++) {
            results[i] = Message.toAll(strs[i].getBytes());
        }
        return results;
    }

    public static void main(String[] args) throws IOException {
        new FunctionServer().registerMapHandler(new FlatMapFunction()).start();
    }
}
