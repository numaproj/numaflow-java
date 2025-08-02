package io.numaproj.numaflow.examples.accumulator.sorter;

import io.numaproj.numaflow.accumulator.Server;
import io.numaproj.numaflow.accumulator.model.Accumulator;
import io.numaproj.numaflow.accumulator.model.AccumulatorFactory;
import io.numaproj.numaflow.accumulator.model.Datum;
import io.numaproj.numaflow.accumulator.model.Message;
import io.numaproj.numaflow.accumulator.model.OutputStreamObserver;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Comparator;
import java.util.TreeSet;

@Slf4j
@AllArgsConstructor
public class StreamSorterFactory extends AccumulatorFactory<StreamSorterFactory.StreamSorter> {

    public static void main(String[] args) throws Exception {
        log.info("Starting stream sorter server..");
        Server server = new Server(new StreamSorterFactory());

        // Start the server
        server.start();

        // wait for the server to shut down
        server.awaitTermination();
        log.info("Stream sorter server exited..");
    }

    @Override
    public StreamSorter createAccumulator() {
        return new StreamSorter();
    }

    public static class StreamSorter extends Accumulator {
        private Instant latestWm = Instant.ofEpochMilli(-1);
        private final TreeSet<Datum> sortedBuffer = new TreeSet<>(Comparator
                .comparing(Datum::getEventTime)
                .thenComparing(Datum::getID)); // Assuming Datum has a getUniqueId() method

        @Override
        public void processMessage(Datum datum, OutputStreamObserver outputStream) {
            log.info("Received datum with event time: {}", datum.toString());
            if (datum.getWatermark().isAfter(latestWm)) {
                latestWm = datum.getWatermark();
                flushBuffer(outputStream);
            }
            sortedBuffer.add(datum);
        }

        @Override
        public void handleEndOfStream(OutputStreamObserver outputStreamObserver) {
            log.info("Eof received, flushing sortedBuffer: {}", latestWm.toEpochMilli());
            flushBuffer(outputStreamObserver);
        }

        private void flushBuffer(OutputStreamObserver outputStream) {
            log.info("Watermark updated, flushing sortedBuffer: {}", latestWm.toEpochMilli());
            while (!sortedBuffer.isEmpty() && sortedBuffer
                    .first()
                    .getEventTime()
                    .isBefore(latestWm)) {
                Datum datum = sortedBuffer.pollFirst();
                assert datum != null;
                outputStream.send(new Message(datum));
                log.info("Sent datum with event time: {}", datum.getEventTime().toEpochMilli());
            }
        }
    }
}
