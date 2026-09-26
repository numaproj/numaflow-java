package io.numaproj.numaflow.examples.sink.tracing;

import io.numaproj.numaflow.examples.tracing.OtelTracing;
import io.numaproj.numaflow.sinker.Datum;
import io.numaproj.numaflow.sinker.DatumIterator;
import io.numaproj.numaflow.sinker.Response;
import io.numaproj.numaflow.sinker.ResponseList;
import io.numaproj.numaflow.sinker.Server;
import io.numaproj.numaflow.sinker.Sinker;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracing-aware log sink UDF example.
 *
 * <p>Emits a {@code user.persist} span per message nested under the Numaflow platform's
 * per-message {@code numaflow.{topology}.sink.write} span — typical place to span an
 * external DB write, HTTP POST, or other persistence call.
 *
 * <p>Required environment variables (set via Pipeline/MonoVertex {@code containerTemplate.env}):
 *
 * <ul>
 *   <li>{@code OTEL_EXPORTER_OTLP_TRACES_ENDPOINT} or {@code OTEL_EXPORTER_OTLP_ENDPOINT}</li>
 *   <li>{@code OTEL_SERVICE_NAME} (optional; defaults to {@code numaflow-udf})</li>
 * </ul>
 */
public class TracingSink extends Sinker {

    private static final Logger log = LoggerFactory.getLogger(TracingSink.class);
    private static final String TRACER_NAME = "numaflow-java-example/sinker-tracing";
    private static final String USER_PERSIST_SPAN = "user.persist";

    public static void main(String[] args) throws Exception {
        OtelTracing.initTracer();
        Server server = new Server(new TracingSink());
        server.start();
        server.awaitTermination();
    }

    @Override
    public ResponseList processMessages(DatumIterator datumIterator) {
        ResponseList.ResponseListBuilder responseListBuilder = ResponseList.newBuilder();
        while (true) {
            Datum datum;
            try {
                datum = datumIterator.next();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                continue;
            }
            if (datum == null) {
                break;
            }

            Context ctx = OtelTracing.extractContext(datum.getSystemMetadata());
            Span span = OtelTracing.getTracer(TRACER_NAME)
                    .spanBuilder(USER_PERSIST_SPAN)
                    .setParent(ctx)
                    .startSpan();
            try (Scope scope = span.makeCurrent()) {
                String msg = new String(datum.getValue());
                log.info("Traced sink: {}, id: {}", msg, datum.getId());
                responseListBuilder.addResponse(Response.responseOK(datum.getId()));
            } catch (Exception e) {
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
