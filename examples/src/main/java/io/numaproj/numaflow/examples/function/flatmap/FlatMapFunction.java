package io.numaproj.numaflow.examples.function.flatmap;

import io.numaproj.numaflow.function.FunctionServer;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.map.MapFunc;
import io.numaproj.numaflow.function.v1.Udfunction;
import java.io.IOException;
import java.util.logging.Logger;

public class FlatMapFunction {

    private static final Logger logger = Logger.getLogger(FlatMapFunction.class.getName());

    private static Message[] process(String key, Udfunction.Datum data) {
        String msg = new String(data.getValue().toByteArray());
        String[] strs = msg.split(",");
        Message[] results = new Message[strs.length];

        for (int i = 0; i < strs.length; i++) {
            results[i] = Message.toAll(strs[i].getBytes());
        }
        return results;
    }

    public static void main(String[] args) throws IOException {
        logger.info("Flatmap invoked");
        new FunctionServer().registerMapper(new MapFunc(FlatMapFunction::process)).start();
    }
}
