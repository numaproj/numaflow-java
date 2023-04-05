package io.numaproj.numaflow.examples.function.reduce.count;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.FunctionServer;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.reduce.ReduceHandler;
import io.numaproj.numaflow.function.reduce.ReducerFactory;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

@Slf4j
@AllArgsConstructor
public class EvenOddCounterFactory extends ReducerFactory<EvenOddCounterFactory.EvenOddCounter> {
    private Config config;

    @Override
    public EvenOddCounter createReducer() {
        return new EvenOddCounter(config);
    }

    @Slf4j
    public static class EvenOddCounter extends ReduceHandler {
        private final Config config;
        private int evenCount;
        private int oddCount;

        public EvenOddCounter(Config config) {
            this.config = config;
        }

        @Override
        public void addMessage(String[] keys, Datum datum, Metadata md) {
            try {
                int val = Integer.parseInt(new String(datum.getValue()));
                // increment based on the value specified in the config
                if (val % 2 == 0) {
                    evenCount += config.getEvenIncrementBy();
                } else {
                    oddCount += config.getOddIncrementBy();
                }
            } catch (NumberFormatException e) {
                log.info("error while parsing integer - {}", e.getMessage());
            }
        }

        @Override
        public Message[] getOutput(String[] keys, Metadata md) {
            log.info(
                    "even and odd count - {} {}, window - {} {}",
                    evenCount,
                    oddCount,
                    md.getIntervalWindow().getStartTime().toString(),
                    md.getIntervalWindow().getEndTime().toString());

            if (Arrays.equals(keys, new String[]{"even"})) {
                return new Message[]{Message.to(keys, String.valueOf(evenCount).getBytes())};
            } else {
                return new Message[]{Message.to(keys, String.valueOf(oddCount).getBytes())};
            }
        }
    }

    public static void main(String[] args) throws IOException {
        log.info("counter udf was invoked");
        Config config = new Config(1, 2);
        new FunctionServer().registerReducerFactory(new EvenOddCounterFactory(config)).start();
    }
}
