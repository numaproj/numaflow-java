package io.numaproj.numaflow.function;

import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.reduce.GroupBy;

import java.nio.ByteBuffer;

public class ReduceTestFn extends GroupBy {
    private int sum = 0;

    public ReduceTestFn(String key, Metadata metadata) {
        super(key, metadata);
    }

    @Override
    public void readMessage(Datum datum) {
        sum += Integer.parseInt(new String(datum.getValue()));
    }

    @Override
    public Message[] getResult() {
        return new Message[]{Message.to(
                key + "-processed",
                String.valueOf(sum).getBytes())};
    }
}
