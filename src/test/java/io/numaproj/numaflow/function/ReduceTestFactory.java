package io.numaproj.numaflow.function;

import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.reduce.Reducer;
import io.numaproj.numaflow.function.reduce.ReducerFactory;

public class ReduceTestFactory extends ReducerFactory<ReduceTestFactory.ReduceTestFn> {
    @Override
    public ReduceTestFn createReducer() {
        return new ReduceTestFn();
    }

    public static class ReduceTestFn extends Reducer {
        private int sum = 0;

        @Override
        public void addMessage(String key, Datum datum, Metadata md) {
            sum += Integer.parseInt(new String(datum.getValue()));
        }

        @Override
        public Message[] getOutput(String key, Metadata md) {
            return new Message[]{Message.to(
                    key + "-processed",
                    String.valueOf(sum).getBytes())};
        }
    }
}
