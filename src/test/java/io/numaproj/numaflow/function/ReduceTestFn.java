package io.numaproj.numaflow.function;

import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.reduce.Reducer;


public class ReduceTestFn extends Reducer {
    private int sum = 0;

    public ReduceTestFn(String key, Metadata metadata) {
        super(key, metadata);
    }

    @Override
    public void addMessage(Datum datum) {
        sum += Integer.parseInt(new String(datum.getValue()));
    }

    @Override
    public Message[] getOutput() {
        return new Message[]{Message.to(
                key + "-processed",
                String.valueOf(sum).getBytes())};
    }
}
