package io.numaproj.numaflow.reducer;


import java.util.Arrays;

public class ReduceTestFactory extends ReducerFactory<ReduceTestFactory.ReduceTestFn> {
    @Override
    public ReduceTestFn createReducer() {
        return new ReduceTestFn();
    }

    public static class ReduceTestFn extends Reducer {
        private int sum = 0;

        @Override
        public void addMessage(String[] keys, Datum datum, Metadata md) {
            sum += Integer.parseInt(new String(datum.getValue()));
        }

        @Override
        public MessageList getOutput(String[] keys, Metadata md) {
            System.out.println("returning number: " + sum);
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
