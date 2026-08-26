package io.numaproj.numaflow.examples.map.tracing;

import io.numaproj.numaflow.examples.tracing.OtelTracing;
import io.numaproj.numaflow.mapper.Datum;
import io.numaproj.numaflow.mapper.Mapper;
import io.numaproj.numaflow.mapper.Message;
import io.numaproj.numaflow.mapper.MessageList;
import io.numaproj.numaflow.mapper.Server;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

/**
 * Tracing-aware pass-through map UDF example.
 *
 * <p>Emits a {@code user.work} span nested under the Numaflow platform's per-message
 * {@code numaflow.{topology}.map} span. Replace the body of {@link #processMessage} with
 * your real work; any further spans started in that scope hang off {@code user.work}.
 *
 * <p>Expected trace tree for a MonoVertex {@code source -> map (this UDF) -> sink}:
 *
 * <pre>
 * numaflow.vertex.process
 * ├── numaflow.monovertex.source.dispatch
 * ├── numaflow.monovertex.map
 * │   └── user.work                          ← emitted by this example
 * └── numaflow.monovertex.sink.write
 * </pre>
 *
 * <p>Required environment variables (set via Pipeline/MonoVertex {@code containerTemplate.env}):
 *
 * <ul>
 *   <li>{@code OTEL_EXPORTER_OTLP_TRACES_ENDPOINT} or {@code OTEL_EXPORTER_OTLP_ENDPOINT}</li>
 *   <li>{@code OTEL_SERVICE_NAME} (optional; defaults to {@code numaflow-udf})</li>
 * </ul>
 */
public class TracingMapFunction extends Mapper {

    private static final String TRACER_NAME = "numaflow-java-example/mapper-tracing";
    private static final String USER_WORK_SPAN = "user.work";

    public static void main(String[] args) throws Exception {
        OtelTracing.initTracer();
        Server server = new Server(new TracingMapFunction());
        server.start();
        server.awaitTermination();
    }

    @Override
    public MessageList processMessage(String[] keys, Datum data) {
        Context ctx = OtelTracing.extractContext(data.getSystemMetadata());
        Span span = OtelTracing.getTracer(TRACER_NAME)
                .spanBuilder(USER_WORK_SPAN)
                .setParent(ctx)
                .startSpan();
        try (Scope scope = span.makeCurrent()) {
            return MessageList.newBuilder()
                    .addMessage(new Message(data.getValue(), keys))
                    .build();
        } finally {
            span.end();
        }
    }
}
