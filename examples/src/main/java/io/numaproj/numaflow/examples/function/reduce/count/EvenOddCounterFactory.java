package io.numaproj.numaflow.examples.function.reduce.count;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.FunctionServer;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.reduce.Reducer;
import io.numaproj.numaflow.function.reduce.ReducerFactory;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
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
    public static class EvenOddCounter extends Reducer {
        private final Config config;
        private int evenCount;
        private int oddCount;

        public EvenOddCounter(Config config) {
            this.config = config;
        }

        @Override
        public void addMessage(String key, Datum datum, Metadata md) {
            try {
                int val = Integer.parseInt(new String(datum.getValue()));
                if (val % 2 == 0) {
                    evenCount += config.getEvenValue();
                } else {
                    oddCount += config.getOddValue();
                }
            } catch (NumberFormatException e) {
                log.info("error while parsing integer - {}", e.getMessage());
            }
        }

        @Override
        public Message[] getOutput(String key, Metadata md) {
            log.info(
                    "even and odd count - {} {}, window - {} {}",
                    evenCount,
                    oddCount,
                    md.getIntervalWindow().getStartTime().toString(),
                    md.getIntervalWindow().getEndTime().toString());

            if (Objects.equals(key, "even")) {
                return new Message[]{Message.to(key, String.valueOf(evenCount).getBytes())};
            } else {
                return new Message[]{Message.to(key, String.valueOf(oddCount).getBytes())};
            }
        }
    }

    public static void main(String[] args) throws IOException {
        log.info("counter udf was invoked");
        Config config = new Config(1, 1);
        new FunctionServer().registerReducerFactory(new EvenOddCounterFactory(config)).start();
    }
}
