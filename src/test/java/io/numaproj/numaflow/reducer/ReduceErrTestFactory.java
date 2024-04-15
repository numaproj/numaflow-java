package io.numaproj.numaflow.reducer;


import java.util.Arrays;

public class ReduceErrTestFactory extends ReducerFactory<ReduceErrTestFactory.ReduceTestFn> {
    @Override
    public ReduceTestFn createReducer() {
        return new ReduceTestFn();
    }

    public static class ReduceTestFn extends Reducer {
        private final int sum = 0;

        @Override
        public void addMessage(String[] keys, Datum datum, Metadata md) {
            throw new RuntimeException("unknown exception");
        }

        @Override
        public MessageList getOutput(String[] keys, Metadata md) {
            String[] updatedKeys = Arrays
                    .stream(keys)
                    .map(c -> c + "-processed")
                    .toArray(String[]::new);
            return MessageList
                    .newBuilder()
                    .addMessage(new Message(String.valueOf(sum).getBytes(), updatedKeys))
                    .build();
        }
    }
}
