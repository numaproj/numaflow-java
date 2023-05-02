package io.numaproj.numaflow.examples.function.reduce.count;

import io.numaproj.numaflow.function.FunctionServer;
import io.numaproj.numaflow.function.handlers.ReduceHandler;
import io.numaproj.numaflow.function.handlers.ReducerFactory;
import io.numaproj.numaflow.function.interfaces.Datum;
import io.numaproj.numaflow.function.interfaces.Metadata;
import io.numaproj.numaflow.function.types.Message;
import io.numaproj.numaflow.function.types.MessageList;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

@Slf4j
@AllArgsConstructor
public class EvenOddCounterFactory extends ReducerFactory<EvenOddCounterFactory.EvenOddCounter> {
    private Config config;

    public static void main(String[] args) throws Exception {
        log.info("counter udf was invoked");
        Config config = new Config(1, 2);
        new FunctionServer().registerReducerFactory(new EvenOddCounterFactory(config)).start();
    }

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
        public MessageList getOutput(String[] keys, Metadata md) {
            log.info(
                    "even and odd count - {} {}, window - {} {}",
                    evenCount,
                    oddCount,
                    md.getIntervalWindow().getStartTime().toString(),
                    md.getIntervalWindow().getEndTime().toString());

            byte[] val;
            if (Arrays.equals(keys, new String[]{"even"})) {
                val = String.valueOf(evenCount).getBytes();
            } else {
                val = String.valueOf(oddCount).getBytes();
            }
            return MessageList
                    .newBuilder()
                    .addMessage(new Message(val))
                    .build();
        }
    }
}
