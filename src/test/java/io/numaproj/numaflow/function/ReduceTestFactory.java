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
        public void addMessage(String[] key, Datum datum, Metadata md) {
            sum += Integer.parseInt(new String(datum.getValue()));
        }

        @Override
        public Message[] getOutput(String[] key, Metadata md) {
            String[] updatedKey = Arrays.stream(key).map(c -> c+"-processed").toArray(String[]::new);
            return new Message[]{Message.to(
                    updatedKey,
                    String.valueOf(sum).getBytes())};
        }
    }
}
