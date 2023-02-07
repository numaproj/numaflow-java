package io.numaproj.numaflow.examples.function.sum;

import io.numaproj.numaflow.function.FunctionServer;
import io.numaproj.numaflow.function.HandlerDatum;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.reduce.GroupBy;

import java.io.IOException;
import java.util.logging.Logger;

public class SumFunction extends GroupBy {
    private static final Logger logger = Logger.getLogger(SumFunction.class.getName());

//    private static Message[] process(String key, ReduceDatumStream reduceDatumStream, Metadata md) {
//        int sum = 0;
//
//        // window information can be accessed using metadata
//        Instant windowStartTime = md.GetIntervalWindow().GetStartTime();
//        Instant windowEndTime = md.GetIntervalWindow().GetEndTime();
//        logger.info("key - " + key);
//
//        while (true) {
//            Datum datum = reduceDatumStream.ReadMessage();
//            // EOF indicates the end of the input
//            if (datum == ReduceDatumStream.EOF) {
//                break;
//            }
//            try {
//                sum += 1;
//            } catch (NumberFormatException e) {
//                logger.severe("unable to convert the value to int, " + e.getMessage());
//            }
//        }
//        return new Message[]{Message.toAll(String.valueOf(sum).getBytes())};
//    }

    private int result = 0;

    public SumFunction(String key, Metadata metadata) {
        super(key, metadata);
    }

    public static void main(String[] args) throws IOException {
        logger.info("counter udf was invoked");
        new FunctionServer().registerReducer(SumFunction.class).start();
    }

    @Override
    public void readMessage(HandlerDatum handlerDatum) {
        result += 1;
    }

    @Override
    public Message[] getResult() {
        return new Message[]{Message.toAll(String.valueOf(result).getBytes())};
    }
}
