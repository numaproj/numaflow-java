package io.numaproj.numaflow.examples.reduce.count;

import io.numaproj.numaflow.reducer.Datum;
import io.numaproj.numaflow.reducer.Message;
import io.numaproj.numaflow.reducer.MessageList;
import io.numaproj.numaflow.reducer.Metadata;
import io.numaproj.numaflow.reducer.Reducer;
import io.numaproj.numaflow.reducer.ReducerFactory;
import io.numaproj.numaflow.reducer.Server;
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
        new Server(new EvenOddCounterFactory(config)).start();
    }

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
