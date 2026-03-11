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
 * SimpleSource emits exactly 100 messages then reports 0 pending.
 * Each read batch is wrapped in an OpenTelemetry span.
 */

@Slf4j
public class SimpleSource extends Sourcer {
    private static final int TOTAL_MESSAGES = 100;

    private final Map<Integer, Boolean> yetToBeAcked = new ConcurrentHashMap<>();
    private final AtomicInteger readIndex = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        TracingUtils.init();

        Server server = new Server(new SimpleSource());
        server.start();
        server.awaitTermination();
    }

    @Override
    public void read(ReadRequest request, OutputObserver observer) {
        log.info("read() called - requested={}, timeout={}ms, readIndex={}, unacked={}, pending={}",
                request.getCount(), request.getTimeout().toMillis(),
                readIndex.get(), yetToBeAcked.size(), getPending());

        Span span = TracingUtils.startSpan("udf.source.read", SpanKind.PRODUCER);
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("source.request.count", request.getCount());
            log.info("  span started: traceId={}, spanId={}",
                    span.getSpanContext().getTraceId(), span.getSpanContext().getSpanId());
            doRead(request, observer);
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            log.error("  read() error: {}", e.getMessage(), e);
            throw e;
        } finally {
            span.end();
        }
    }

    private void doRead(ReadRequest request, OutputObserver observer) {
        if (!yetToBeAcked.isEmpty()) {
            log.info("  skipping read - {} messages still unacked", yetToBeAcked.size());
            return;
        }

        int remaining = TOTAL_MESSAGES - readIndex.get();
        if (remaining <= 0) {
            log.info("  all {} messages already emitted", TOTAL_MESSAGES);
            return;
        }

        int count = (int) Math.min(request.getCount(), remaining);
        log.info("  emitting {} messages (remaining={})", count, remaining);

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            if (System.currentTimeMillis() - startTime > request.getTimeout().toMillis()) {
                log.info("  timeout after emitting {} of {} messages", i, count);
                return;
            }
            Integer index = readIndex.incrementAndGet();
            observer.send(constructMessage(index));
            yetToBeAcked.put(index, true);
        }
        log.info("  emitted {} messages, readIndex now {}", count, readIndex.get());
    }

    @Override
    public void ack(AckRequest request) {
        log.info("ack() called - offsets={}", request.getOffsets().size());
        for (Offset offset : request.getOffsets()) {
            int decoded = ByteBuffer.wrap(offset.getValue()).getInt();
            yetToBeAcked.remove(decoded);
        }
        log.info("  after ack: unacked={}, pending={}", yetToBeAcked.size(), getPending());
    }

    @Override
    public void nack(NackRequest request) {
        log.info("nack() called - offsets={}", request.getOffsets().size());
        for (Offset offset : request.getOffsets()) {
            int decoded = ByteBuffer.wrap(offset.getValue()).getInt();
            yetToBeAcked.remove(decoded);
        }
        log.info("  after nack: unacked={}, pending={}", yetToBeAcked.size(), getPending());
    }

    @Override
    public long getPending() {
        long remaining = TOTAL_MESSAGES - readIndex.get() + yetToBeAcked.size();
        return Math.max(remaining, 0);
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
