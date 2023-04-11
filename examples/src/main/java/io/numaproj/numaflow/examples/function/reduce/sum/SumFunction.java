package io.numaproj.numaflow.examples.function.reduce.sum;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.MessageList;
import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.reduce.ReduceHandler;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SumFunction extends ReduceHandler {

    private int sum = 0;

    @Override
    public void addMessage(String[] keys, Datum datum, Metadata md) {
        try {
            sum += Integer.parseInt(new String(datum.getValue()));
        } catch (NumberFormatException e) {
            log.info("error while parsing integer - {}", e.getMessage());
        }
    }

    @Override
    public MessageList getOutput(String[] keys, Metadata md) {
        return MessageList
                .newBuilder()
                .addMessage(Message.newBuilder().value(String.valueOf(sum).getBytes()).build())
                .build();
    }
}
