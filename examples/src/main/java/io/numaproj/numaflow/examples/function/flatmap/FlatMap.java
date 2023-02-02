package io.numaproj.numaflow.examples.function.flatmap;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.FunctionServer;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.map.MapFunc;

import java.io.IOException;
import java.util.logging.Logger;

public class FlatMap {
    private static final Logger logger = Logger.getLogger(FlatMap.class.getName());

    private static Message[] process(String key, Datum data) {
        String msg = new String(data.getValue());
        String[] strs = msg.split(",");
        Message[] results = new Message[strs.length];

        for (int i = 0; i < strs.length; i++) {
            results[i] = Message.toAll(strs[i].getBytes());
        }
        return results;
    }

    public static void main(String[] args) throws IOException {
        logger.info("Flatmap invoked");
        new FunctionServer().registerMapper(new MapFunc(FlatMap::process)).start();
    }
}
