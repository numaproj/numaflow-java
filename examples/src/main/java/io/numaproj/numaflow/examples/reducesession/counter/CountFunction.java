package io.numaproj.numaflow.examples.reducesession.counter;

import io.numaproj.numaflow.sessionreducer.model.Datum;
import io.numaproj.numaflow.sessionreducer.model.Message;
import io.numaproj.numaflow.sessionreducer.model.SessionReducer;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * CountFunction is a simple session reducer which counts the number of events in a session.
 */
@Slf4j
public class CountFunction extends SessionReducer {

    private final AtomicInteger count = new AtomicInteger(0);

    @Override
    public void processMessage(
            String[] keys,
            Datum datum,
            io.numaproj.numaflow.sessionreducer.model.OutputStreamObserver outputStreamObserver) {
        this.count.incrementAndGet();
    }

    @Override
    public void handleEndOfStream(
            String[] keys,
            io.numaproj.numaflow.sessionreducer.model.OutputStreamObserver outputStreamObserver) {
        outputStreamObserver.send(new Message(String.valueOf(this.count.get()).getBytes()));
    }

    @Override
    public byte[] accumulator() {
        return String.valueOf(this.count.get()).getBytes();
    }

    @Override
    public void mergeAccumulator(byte[] accumulator) {
        int value = 0;
        try {
            value = Integer.parseInt(new String(accumulator));
        } catch (NumberFormatException e) {
            log.info("error while parsing integer - {}", e.getMessage());
        }
        this.count.addAndGet(value);
    }
}
