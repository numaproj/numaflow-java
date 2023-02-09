package io.numaproj.numaflow.examples.function.evenodd;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.FunctionServer;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.map.MapFunc;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.logging.Logger;

@Slf4j
public class Forward {

    private static Message[] process(String key, Datum data) {
        int value = Integer.parseInt(new String(data.getValue()));
        if (value % 2 == 0) {
            return new Message[]{Message.to("even", data.getValue())};
        }
        return new Message[]{Message.to("odd", data.getValue())};
    }

    public static void main(String[] args) throws IOException {
        log.info("Forward invoked");
        new FunctionServer().registerMapper(new MapFunc(Forward::process)).start();
    }
}
