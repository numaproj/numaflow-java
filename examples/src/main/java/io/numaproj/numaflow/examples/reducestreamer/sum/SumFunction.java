package io.numaproj.numaflow.examples.reducestreamer.sum;

import io.numaproj.numaflow.reducestreamer.model.Message;
import io.numaproj.numaflow.reducestreamer.user.OutputStreamObserver;
import io.numaproj.numaflow.reducestreamer.user.ReduceStreamer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SumFunction extends ReduceStreamer {

    private int sum = 0;

    @Override
    public void processMessage(
            String[] keys,
            io.numaproj.numaflow.reducestreamer.model.Datum datum,
            OutputStreamObserver outputStream,
            io.numaproj.numaflow.reducestreamer.model.Metadata md) {
        try {
            sum += Integer.parseInt(new String(datum.getValue()));
        } catch (NumberFormatException e) {
            log.info("error while parsing integer - {}", e.getMessage());
        }
        if (sum >= 100) {
            outputStream.send(new Message(String.valueOf(sum).getBytes()));
            sum = 0;
        }
    }
}
