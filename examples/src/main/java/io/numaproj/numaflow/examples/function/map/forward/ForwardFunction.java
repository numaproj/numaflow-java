package io.numaproj.numaflow.examples.function.map.forward;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.map.MapFunc;
import io.numaproj.numaflow.function.server.FunctionServer;

import java.io.IOException;

public class ForwardFunction {
    private static Message[] process(String key, Datum data) {
        return new Message[]{Message.toAll(data.getValue())};
    }

    public static void main(String[] args) throws IOException {
        new FunctionServer().registerMapper(new MapFunc(ForwardFunction::process)).start();
    }
}
