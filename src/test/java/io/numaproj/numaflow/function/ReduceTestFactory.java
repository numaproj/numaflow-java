package io.numaproj.numaflow.function;

import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.reduce.ReduceHandler;
import io.numaproj.numaflow.function.reduce.ReducerFactory;

import java.util.Arrays;

public class ReduceTestFactory extends ReducerFactory<ReduceTestFactory.ReduceTestFn> {
    @Override
    public ReduceTestFn createReducer() {
        return new ReduceTestFn();
    }

    public static class ReduceTestFn extends ReduceHandler {
        private int sum = 0;

        @Override
        public void addMessage(String[] keys, Datum datum, Metadata md) {
            sum += Integer.parseInt(new String(datum.getValue()));
        }

        @Override
        public MessageList getOutput(String[] keys, Metadata md) {
            String[] updatedKeys = Arrays
                    .stream(keys)
                    .map(c -> c + "-processed")
                    .toArray(String[]::new);
            return MessageList
                    .builder()
                    .addMessage(Message
                            .builder()
                            .keys(updatedKeys)
                            .value(String.valueOf(sum).getBytes())
                            .build())
                    .build();
        }
    }
}
