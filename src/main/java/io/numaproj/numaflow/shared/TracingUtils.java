package io.numaproj.numaflow.shared;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * TracingUtils provides OpenTelemetry tracing utilities for numaflow UDFs.
 * <p>
 * It extracts W3C Trace Context (traceparent/tracestate) from {@link SystemMetadata}
 * and creates child spans that participate in the distributed trace propagated
 * through numaflow-core via {@code sys_metadata["tracing"]}.
 * <p>
 * Env vars read by {@link #init()}:
 * <ul>
 *   <li>{@code OPENTEL_ENABLED} -- set to {@code "true"} to activate tracing (gate switch)</li>
 *   <li>{@code OTEL_EXPORTER_OTLP_ENDPOINT} -- OTLP gRPC endpoint (e.g. {@code http://<host>:4317})</li>
 *   <li>{@code OTEL_SERVICE_NAME} -- service name for spans</li>
 * </ul>
 * If {@code OPENTEL_ENABLED} is not {@code "true"}, or no endpoint is configured,
 * all tracing operations are effectively no-ops.
 */
public final class TracingUtils {
    private static final Logger log = LoggerFactory.getLogger(TracingUtils.class);

    private static final String TRACING_GROUP = "tracing";
    private static final String INSTRUMENTATION_NAME = "numaflow-java-sdk";
    private static final String OPENTEL_ENABLED_ENV = "OPENTEL_ENABLED";

    private static volatile boolean initialized = false;

    private static final TextMapGetter<Map<String, String>> MAP_GETTER =
            new TextMapGetter<Map<String, String>>() {
                @Override
                public Iterable<String> keys(Map<String, String> carrier) {
                    return carrier.keySet();
                }

                @Override
                public String get(Map<String, String> carrier, String key) {
                    return carrier == null ? null : carrier.get(key);
                }
            };

    private TracingUtils() {}

    /**
     * Initialize the OpenTelemetry SDK from environment variables.
     * Gated by {@code OPENTEL_ENABLED=true}; if absent or not "true", tracing stays no-op.
     * When enabled, reads {@code OTEL_EXPORTER_OTLP_ENDPOINT}, {@code OTEL_SERVICE_NAME}, etc.
     * Safe to call multiple times; initialization happens only on the first call.
     */
    public static synchronized void init() {
        if (initialized) {
            return;
        }
        String enabled = System.getenv(OPENTEL_ENABLED_ENV);
        if (!"true".equalsIgnoreCase(enabled)) {
            log.info("OpenTelemetry tracing disabled ({}={})", OPENTEL_ENABLED_ENV, enabled);
            initialized = true;
            return;
        }
        try {
            AutoConfiguredOpenTelemetrySdk.builder()
                    .setResultAsGlobal()
                    .build();
            initialized = true;
            log.info("OpenTelemetry SDK initialized (endpoint={}, service={})",
                    System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
                    System.getenv("OTEL_SERVICE_NAME"));
        } catch (Exception e) {
            log.warn("Failed to initialize OpenTelemetry SDK, tracing will be no-op: {}",
                    e.getMessage());
        }
    }

    /**
     * Get a {@link Tracer} instance for creating spans.
     * If {@link #init()} has not been called, returns a no-op tracer.
     */
    public static Tracer getTracer() {
        return GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME);
    }

    /**
     * Extract a parent {@link Context} from the W3C trace headers stored in
     * {@code sys_metadata["tracing"]} of the given {@link SystemMetadata}.
     *
     * @param systemMetadata the system metadata from an incoming request
     * @return the extracted parent context, or {@link Context#current()} if no trace context is found
     */
    public static Context extractContext(SystemMetadata systemMetadata) {
        if (systemMetadata == null) {
            return Context.current();
        }

        Map<String, String> tracingMap = new HashMap<>();

        byte[] traceparent = systemMetadata.getValue(TRACING_GROUP, "traceparent");
        if (traceparent != null) {
            tracingMap.put("traceparent", new String(traceparent, StandardCharsets.UTF_8));
        }

        byte[] tracestate = systemMetadata.getValue(TRACING_GROUP, "tracestate");
        if (tracestate != null) {
            tracingMap.put("tracestate", new String(tracestate, StandardCharsets.UTF_8));
        }

        if (tracingMap.isEmpty()) {
            return Context.current();
        }

        TextMapPropagator propagator = GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
        return propagator.extract(Context.current(), tracingMap, MAP_GETTER);
    }

    /**
     * Start a child span by extracting the parent trace context from {@link SystemMetadata}.
     * The returned {@link Span} must be ended by the caller via {@link Span#end()}.
     *
     * @param spanName       name for the span (e.g. "udf.sink", "udf.map")
     * @param systemMetadata the system metadata carrying trace context
     * @param kind           the span kind (typically {@link SpanKind#SERVER} for UDFs)
     * @return a started {@link Span}
     */
    public static Span startSpan(String spanName, SystemMetadata systemMetadata, SpanKind kind) {
        Context parentCtx = extractContext(systemMetadata);
        return getTracer().spanBuilder(spanName)
                .setParent(parentCtx)
                .setSpanKind(kind)
                .startSpan();
    }

    /**
     * Start a root span with no parent context.
     * Useful for source UDFs that originate traces.
     * The returned {@link Span} must be ended by the caller via {@link Span#end()}.
     *
     * @param spanName name for the span (e.g. "udf.source.read")
     * @param kind     the span kind (typically {@link SpanKind#PRODUCER} for sources)
     * @return a started {@link Span}
     */
    public static Span startSpan(String spanName, SpanKind kind) {
        return getTracer().spanBuilder(spanName)
                .setSpanKind(kind)
                .startSpan();
    }
}
