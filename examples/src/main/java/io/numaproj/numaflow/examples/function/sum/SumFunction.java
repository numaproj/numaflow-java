package io.numaproj.numaflow.examples.function.sum;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.FunctionServer;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.reduce.Reducer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class SumFunction extends Reducer {

    private int sum = 0;

    public SumFunction(String key, Metadata metadata) {
        super(key, metadata);
    }

    public static void main(String[] args) throws IOException {
        log.info("counter udf was invoked");
        new FunctionServer().registerReducer(SumFunction.class).start();
    }

    @Override
    public void addMessage(Datum datum) {
        try {
            sum += Integer.parseInt(new String(datum.getValue()));
        } catch (NumberFormatException e) {
            log.info("error while parsing integer - {}", e.getMessage());
        }
    }

    @Override
    public Message[] getOutput() {
        return new Message[]{Message.toAll(String.valueOf(sum).getBytes())};
    }
}
