package io.numaproj.numaflow.examples.function.sum;

import io.numaproj.numaflow.function.FunctionServer;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.reduce.ReduceDatumStream;
import io.numaproj.numaflow.function.reduce.ReduceFunc;
import io.numaproj.numaflow.function.v1.Udfunction;

import java.io.IOException;
import java.time.Instant;
import java.util.logging.Logger;

public class SumFunction {
    private static final Logger logger = Logger.getLogger(SumFunction.class.getName());

    private static Message[] process(String key, ReduceDatumStream reduceDatumStream, Metadata md) {
        int sum = 0;

        // window information can be accessed using metadata
        Instant windowStartTime = md.GetIntervalWindow().GetStartTime();
        Instant windowEndTime = md.GetIntervalWindow().GetEndTime();

        while (true) {
            Udfunction.Datum datum = reduceDatumStream.ReadMessage();
            // null indicates the end of the input
            if (datum == ReduceDatumStream.EOF) {
                break;
            }
            try {
                sum += Integer.parseInt(new String(datum.getValue().toByteArray()));
            } catch (NumberFormatException e) {
                logger.severe("unable to convert the value to int, " + e.getMessage());
            }
        }
        return new Message[]{Message.toAll(String.valueOf(sum).getBytes())};
    }

    public static void main(String[] args) throws IOException {
        new FunctionServer().registerReducer(new ReduceFunc(SumFunction::process)).start();
    }
}
