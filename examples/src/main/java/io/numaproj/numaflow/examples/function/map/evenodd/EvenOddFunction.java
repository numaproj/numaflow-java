package io.numaproj.numaflow.examples.function.map.evenodd;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.FunctionServer;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.map.MapHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * This is a simple User Defined Function example which receives a message,
 * and attaches a key to the message based on the value, if the value is even
 * the key will be set as "even" if the value is odd the key will be set as
 * "odd"
 */

@Slf4j
public class EvenOddFunction extends MapHandler {

    public Message[] processMessage(String[] key, Datum data) {
        int value = 0;
        try {
            value = Integer.parseInt(new String(data.getValue()));
        } catch (NumberFormatException e) {
            log.error("Error occurred while parsing int");
            return new Message[]{Message.toDrop()};
        }
        if (value % 2 == 0) {
            return new Message[]{Message.to(new String[]{"even"}, data.getValue())};
        }
        return new Message[]{Message.to(new String[]{"odd"}, data.getValue())};
    }

    public static void main(String[] args) throws IOException {
        new FunctionServer().registerMapHandler(new EvenOddFunction()).start();
    }
}
