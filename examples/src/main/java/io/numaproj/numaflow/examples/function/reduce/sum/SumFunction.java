package io.numaproj.numaflow.examples.function.reduce.sum;

import io.numaproj.numaflow.reducer.Datum;
import io.numaproj.numaflow.reducer.Message;
import io.numaproj.numaflow.reducer.MessageList;
import io.numaproj.numaflow.reducer.Metadata;
import io.numaproj.numaflow.reducer.Reducer;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SumFunction extends Reducer {

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
                .addMessage(new Message(String.valueOf(sum).getBytes()))
                .build();
    }
}
