package io.numaproj.numaflow.examples.function.map.evenodd;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.map.MapFunc;
import io.numaproj.numaflow.function.server.FunctionServer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class EvenOddFunction {

    private static Message[] process(String key, Datum data) {
        int value = 0;
        try {
            value = Integer.parseInt(new String(data.getValue()));
        } catch (NumberFormatException e) {
            log.error("Error occurred while parsing int");
            return new Message[]{Message.toDrop()};
        }
        if (value % 2 == 0) {
            return new Message[]{Message.to("even", data.getValue())};
        }
        return new Message[]{Message.to("odd", data.getValue())};
    }

    public static void main(String[] args) throws IOException {
        new FunctionServer().registerMapper(new MapFunc(EvenOddFunction::process)).start();
    }
}
