package io.numaproj.numaflow.examples.tracing;

import io.numaproj.numaflow.shared.SystemMetadata;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Shared OpenTelemetry wiring for Numaflow UDF tracing examples.
 *
 * <p>The Numaflow data plane injects the platform's per-stage trace context into
 * {@code sys_metadata["tracing_udf"]} (W3C traceparent + optional tracestate) before
 * calling the UDF. These helpers:
 *
 * <ol>
 *   <li>Initialise an OTLP gRPC tracer in the UDF process ({@link #initTracer()}).</li>
 *   <li>Extract the platform parent context from each message ({@link #extractContext(SystemMetadata)}).</li>
 *   <li>Allow user-defined child spans to nest under the platform stage span.</li>
 * </ol>
 *
 * <p>Required environment variables (set on the Pipeline/MonoVertex containerTemplate):
 *
 * <ul>
 *   <li>{@code OTEL_EXPORTER_OTLP_TRACES_ENDPOINT} or {@code OTEL_EXPORTER_OTLP_ENDPOINT}</li>
 *   <li>{@code OTEL_SERVICE_NAME} (optional; defaults to {@code numaflow-udf})</li>
 * </ul>
 *
 * <p>When neither endpoint variable is set, {@link #initTracer()} is a no-op and span
 * creation in the UDF body is essentially free.
 */
public final class OtelTracing {

    private static final String TRACING_UDF_GROUP = "tracing_udf";

    private static SdkTracerProvider tracerProvider;
    private static OpenTelemetry openTelemetry;

    private OtelTracing() {
    }

    /**
     * Wires an OTLP gRPC tracer provider and the W3C propagator.
     *
     * <p>Registers a JVM shutdown hook so spans are flushed before exit.
     */
    public static void initTracer() {
        String endpoint = System.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
        if (endpoint == null || endpoint.isEmpty()) {
            endpoint = System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
        }
        if (endpoint == null || endpoint.isEmpty()) {
            System.out.println("[tracing] OTLP endpoint not set; UDF spans will be no-ops");
            return;
        }

        String serviceName = System.getenv("OTEL_SERVICE_NAME");
        if (serviceName == null || serviceName.isEmpty()) {
            serviceName = "numaflow-udf";
        }

        OtlpGrpcSpanExporter exporter = OtlpGrpcSpanExporter.builder()
                .setEndpoint(endpoint)
                .build();

        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(
                        AttributeKey.stringKey("service.name"),
                        serviceName
                )));

        tracerProvider = SdkTracerProvider.builder()
                .setSampler(Sampler.parentBased(Sampler.alwaysOn()))
                .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
                .setResource(resource)
                .build();

        openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .buildAndRegisterGlobal();

        System.out.printf(
                "[tracing] OTLP exporter configured: endpoint=%s service=%s%n",
                endpoint,
                serviceName
        );

        Runtime.getRuntime().addShutdownHook(new Thread(OtelTracing::shutdown));
    }

    /**
     * Shuts down the tracer provider so batched spans are flushed.
     */
    public static void shutdown() {
        if (tracerProvider != null) {
            tracerProvider.shutdown().join(5, TimeUnit.SECONDS);
            tracerProvider = null;
            openTelemetry = null;
        }
    }

    /**
     * Returns a tracer for the given instrumentation scope.
     */
    public static Tracer getTracer(String instrumentationName) {
        if (openTelemetry != null) {
            return openTelemetry.getTracer(instrumentationName);
        }
        return OpenTelemetry.noop().getTracer(instrumentationName);
    }

    /**
     * Reads the W3C traceparent/tracestate the platform wrote into
     * {@code sys_metadata["tracing_udf"]} and returns a context whose current span is
     * the platform-side stage span.
     *
     * <p>Safe to call when tracing is disabled or when no parent context is present.
     */
    public static Context extractContext(SystemMetadata systemMetadata) {
        if (systemMetadata == null || openTelemetry == null) {
            return Context.current();
        }

        byte[] traceparentBytes = systemMetadata.getValue(TRACING_UDF_GROUP, "traceparent");
        if (traceparentBytes == null || traceparentBytes.length == 0) {
            return Context.current();
        }

        Map<String, String> carrier = new HashMap<>();
        carrier.put("traceparent", new String(traceparentBytes, StandardCharsets.UTF_8));

        byte[] tracestateBytes = systemMetadata.getValue(TRACING_UDF_GROUP, "tracestate");
        if (tracestateBytes != null && tracestateBytes.length > 0) {
            carrier.put("tracestate", new String(tracestateBytes, StandardCharsets.UTF_8));
        }

        return openTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), carrier, MapGetter.INSTANCE);
    }

    private enum MapGetter implements TextMapGetter<Map<String, String>> {
        INSTANCE;

        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier.get(key);
        }
    }
}
