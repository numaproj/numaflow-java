package io.numaproj.numaflow.examples.sink.simple;

import io.numaproj.numaflow.shared.TracingUtils;
import io.numaproj.numaflow.sinker.Datum;
import io.numaproj.numaflow.sinker.DatumIterator;
import io.numaproj.numaflow.sinker.Response;
import io.numaproj.numaflow.sinker.ResponseList;
import io.numaproj.numaflow.sinker.Server;
import io.numaproj.numaflow.sinker.Sinker;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import lombok.extern.slf4j.Slf4j;


/**
 * Simple User Defined Sink with OpenTelemetry tracing.
 * Extracts parent trace context from sys_metadata["tracing"] on each datum
 * and creates a child span for the sink write operation.
 */

@Slf4j
public class SimpleSink extends Sinker {

    public static void main(String[] args) throws Exception {
        TracingUtils.init();

        Server server = new Server(new SimpleSink());
        server.start();
        server.awaitTermination();
    }

    @Override
    public ResponseList processMessages(DatumIterator datumIterator) {
        ResponseList.ResponseListBuilder responseListBuilder = ResponseList.newBuilder();
        while (true) {
            Datum datum = null;
            try {
                datum = datumIterator.next();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                continue;
            }
            if (datum == null) {
                break;
            }

            log.info("Received message: {}, headers - {}", new String(datum.getValue()), datum.getHeaders());
            log.info("  systemMetadata groups: {}", datum.getSystemMetadata() != null
                    ? datum.getSystemMetadata().getGroups() : "null");
            if (datum.getSystemMetadata() != null) {
                for (String group : datum.getSystemMetadata().getGroups()) {
                    log.info("  sys_metadata[{}] keys: {}", group, datum.getSystemMetadata().getKeys(group));
                    for (String key : datum.getSystemMetadata().getKeys(group)) {
                        byte[] val = datum.getSystemMetadata().getValue(group, key);
                        log.info("    {}={}", key, val != null ? new String(val) : "null");
                    }
                }
            }
            log.info("  userMetadata groups: {}", datum.getUserMetadata() != null
                    ? datum.getUserMetadata().getGroups() : "null");

            Span span = TracingUtils.startSpan(
                    "udf.sink.write", datum.getSystemMetadata(), SpanKind.SERVER);
            try (Scope scope = span.makeCurrent()) {
                span.setAttribute("sink.message.id", datum.getId());
                responseListBuilder.addResponse(Response.responseOK(datum.getId()));
            } catch (Exception e) {
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.recordException(e);
                responseListBuilder.addResponse(Response.responseFailure(
                        datum.getId(),
                        e.getMessage()));
            } finally {
                span.end();
            }
        }
        return responseListBuilder.build();
    }
}
