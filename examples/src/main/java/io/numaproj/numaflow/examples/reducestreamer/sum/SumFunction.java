package io.numaproj.numaflow.examples.reducestreamer.sum;

import io.numaproj.numaflow.reducestreamer.model.Datum;
import io.numaproj.numaflow.reducestreamer.model.Message;
import io.numaproj.numaflow.reducestreamer.model.Metadata;
import io.numaproj.numaflow.reducestreamer.model.OutputStreamObserver;
import io.numaproj.numaflow.reducestreamer.model.ReduceStreamer;
import lombok.extern.slf4j.Slf4j;

/**
 * SumFunction is a User Defined Reduce Stream Function example which sums up the values for the given keys
 * and outputs the sum when the sum is greater than 100.
 * When the input stream closes, the function outputs the sum no matter what value it holds.
 */
@Slf4j
public class SumFunction extends ReduceStreamer {

    private int sum = 0;

    @Override
    public void processMessage(
            String[] keys,
            Datum datum,
            OutputStreamObserver outputStreamObserver,
            Metadata md) {
        try {
            sum += Integer.parseInt(new String(datum.getValue()));
        } catch (NumberFormatException e) {
            log.info("error while parsing integer - {}", e.getMessage());
        }
        if (sum >= 100) {
            outputStreamObserver.send(new Message(String.valueOf(sum).getBytes(), keys));
            sum = 0;
        }
    }

    @Override
    public void handleEndOfStream(
            String[] keys,
            OutputStreamObserver outputStreamObserver,
            Metadata md) {
        outputStreamObserver.send(new Message(String.valueOf(sum).getBytes()));
    }
}
