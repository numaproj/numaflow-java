package io.numaproj.numaflow.examples.accumulator.blackhole;

import io.numaproj.numaflow.accumulator.Server;
import io.numaproj.numaflow.accumulator.model.Accumulator;
import io.numaproj.numaflow.accumulator.model.AccumulatorFactory;
import io.numaproj.numaflow.accumulator.model.Datum;
import io.numaproj.numaflow.accumulator.model.Message;
import io.numaproj.numaflow.accumulator.model.OutputStreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * Blackhole is an accumulator that intentionally discards every datum it receives without
 * forwarding any data downstream.
 *
 * <p>A naive implementation would simply read the input stream and emit nothing. However, an
 * accumulator that never emits anything for the datums it consumes leaves the framework unable to
 * release the per-datum tracked state, leading to unbounded memory growth.
 *
 * <p>Instead, this example emits a drop message for every datum using {@link Message#toDrop(Datum)}.
 * A drop message is not forwarded to the next vertex, but it still allows the framework to advance
 * the watermark and release the tracked state for that datum - giving us "blackhole" semantics
 * without leaking memory. This pattern is useful for multiplexer-, cross-join-, or filter-style
 * accumulators that legitimately need to omit some (or all) of their inputs.
 */
@Slf4j
public class BlackholeFactory extends AccumulatorFactory<BlackholeFactory.Blackhole> {

    public static void main(String[] args) throws Exception {
        log.info("Starting blackhole accumulator server..");
        Server server = new Server(new BlackholeFactory());

        // Start the server
        server.start();

        // wait for the server to shut down
        server.awaitTermination();
        log.info("Blackhole accumulator server exited..");
    }

    @Override
    public Blackhole createAccumulator() {
        return new Blackhole();
    }

    public static class Blackhole extends Accumulator {
        @Override
        public void processMessage(Datum datum, OutputStreamObserver outputStream) {
            log.info(
                    "Dropping datum with event time: {}, watermark: {}",
                    datum.getEventTime().toEpochMilli(),
                    datum.getWatermark().toEpochMilli());
            // Emit a drop message: nothing is forwarded downstream, but the framework still
            // advances the watermark and releases the tracked state for this datum.
            outputStream.send(Message.toDrop(datum));
        }

        @Override
        public void handleEndOfStream(OutputStreamObserver outputStreamObserver) {
            log.info("End of stream received, nothing to flush for blackhole accumulator");
        }
    }
}
