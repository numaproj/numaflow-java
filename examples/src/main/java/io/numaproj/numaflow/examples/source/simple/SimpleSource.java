package io.numaproj.numaflow.examples.source.simple;

import io.numaproj.numaflow.shared.TracingUtils;
import io.numaproj.numaflow.sourcer.AckRequest;
import io.numaproj.numaflow.sourcer.Message;
import io.numaproj.numaflow.sourcer.NackRequest;
import io.numaproj.numaflow.sourcer.Offset;
import io.numaproj.numaflow.sourcer.OutputObserver;
import io.numaproj.numaflow.sourcer.ReadRequest;
import io.numaproj.numaflow.sourcer.Server;
import io.numaproj.numaflow.sourcer.Sourcer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SimpleSource is a simple implementation of Sourcer with OpenTelemetry tracing.
 * It generates messages with increasing offsets.
 * Keeps track of the offsets of the messages read and
 * acknowledges them when ack is called.
 */

@Slf4j
public class SimpleSource extends Sourcer {
    private final Map<Integer, Boolean> yetToBeAcked = new ConcurrentHashMap<>();
    Map<Integer, Boolean> nacked = new ConcurrentHashMap<>();
    private final AtomicInteger readIndex = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        TracingUtils.init();

        Server server = new Server(new SimpleSource());
        server.start();
        server.awaitTermination();
    }

    @Override
    public void read(ReadRequest request, OutputObserver observer) {
        Span span = TracingUtils.startSpan("udf.source.read", SpanKind.PRODUCER);
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("source.request.count", request.getCount());
            doRead(request, observer);
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    private void doRead(ReadRequest request, OutputObserver observer) {
        long startTime = System.currentTimeMillis();

        if (!nacked.isEmpty()) {
            for (int i = 0; i < nacked.size(); i++) {
                Integer index = readIndex.incrementAndGet();
                yetToBeAcked.put(index, true);
                observer.send(constructMessage(index));
            }
            nacked.clear();
        }

        if (!yetToBeAcked.isEmpty()) {
            return;
        }

        for (int i = 0; i < request.getCount(); i++) {
            if (System.currentTimeMillis() - startTime > request.getTimeout().toMillis()) {
                return;
            }

            Integer index = readIndex.incrementAndGet();
            observer.send(constructMessage(index));
            yetToBeAcked.put(index, true);
        }
    }

    @Override
    public void ack(AckRequest request) {
        for (Offset offset : request.getOffsets()) {
            Integer decoded_offset = ByteBuffer.wrap(offset.getValue()).getInt();
            yetToBeAcked.remove(decoded_offset);
        }
    }

    @Override
    public void nack(NackRequest request) {
        for (Offset offset : request.getOffsets()) {
            Integer decoded_offset = ByteBuffer.wrap(offset.getValue()).getInt();
            yetToBeAcked.remove(decoded_offset);
            nacked.put(decoded_offset, true);
            readIndex.decrementAndGet();
        }
    }

    @Override
    public long getPending() {
        return yetToBeAcked.size();
    }

    @Override
    public List<Integer> getPartitions() {
        return Sourcer.defaultPartitions();
    }

    private Message constructMessage(Integer readIndex) {
        Map<String, String> headers = new HashMap<>();
        headers.put("x-txn-id", UUID.randomUUID().toString());

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(readIndex);
        Offset offset = new Offset(buffer.array());
        return new Message(
                Integer.toString(readIndex).getBytes(),
                offset,
                Instant.now(),
                headers);
    }
}
