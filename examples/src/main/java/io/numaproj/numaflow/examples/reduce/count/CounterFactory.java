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

/**
 * CounterFactory is used to create Counter Reducer.
 * Counter Reducer is used to count the number of messages received.
 * it increments the count based on the value specified in the config.
 */
@Slf4j
@AllArgsConstructor
public class CounterFactory extends ReducerFactory<CounterFactory.Counter> {
    private Config config;

    public static void main(String[] args) throws Exception {
        log.info("counter udf was invoked");
        Config config = new Config(1);
        Server server = new Server(new CounterFactory(config));

        // Start the server
        server.start();

        // wait for the server to shut down
        server.awaitTermination();
    }

    @Override
    public Counter createReducer() {
        return new Counter(config);
    }

    @Slf4j
    public static class Counter extends Reducer {
        private final Config config;
        private int count;

        public Counter(Config config) {
            this.config = config;
        }

        @Override
        public void addMessage(String[] keys, Datum datum, Metadata md) {
            // increment based on the value specified in the config
            count += config.getIncrementBy();
        }

        @Override
        public MessageList getOutput(String[] keys, Metadata md) {
            log.info(
                    "count - {}, window - {} {}",
                    count,
                    md.getIntervalWindow().getStartTime().toString(),
                    md.getIntervalWindow().getEndTime().toString());

            return MessageList
                    .newBuilder()
                    .addMessage(new Message(String.valueOf(count).getBytes(), keys))
                    .build();
        }
    }
}
