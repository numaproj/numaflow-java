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

            Span span = TracingUtils.startSpan(
                    "udf.sink.write", datum.getSystemMetadata(), SpanKind.SERVER);
            try (Scope scope = span.makeCurrent()) {
                String msg = new String(datum.getValue());
                span.setAttribute("sink.message.id", datum.getId());
                log.info("Received message: {}, headers - {}", msg, datum.getHeaders());
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
