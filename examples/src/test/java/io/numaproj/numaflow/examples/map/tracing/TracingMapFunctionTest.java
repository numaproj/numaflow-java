package io.numaproj.numaflow.examples.map.tracing;

import com.google.protobuf.ByteString;
import common.MetadataOuterClass;
import io.numaproj.numaflow.mapper.MapperTestKit;
import io.numaproj.numaflow.mapper.Message;
import io.numaproj.numaflow.mapper.MessageList;
import io.numaproj.numaflow.shared.SystemMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TracingMapFunctionTest {

    private static final String TRACEPARENT =
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @Test
    public void testPassThroughWithoutTracingMetadata() {
        MapperTestKit.TestDatum datum = MapperTestKit.TestDatum.builder()
                .value("hello".getBytes())
                .build();

        TracingMapFunction function = new TracingMapFunction();
        MessageList result = function.processMessage(new String[]{}, datum);

        List<Message> messages = result.getMessages();
        Assertions.assertEquals(1, messages.size());
        Assertions.assertEquals("hello", new String(messages.get(0).getValue()));
    }

    @Test
    public void testPassThroughWithTracingMetadata() {
        SystemMetadata systemMetadata = tracingSystemMetadata(TRACEPARENT, "vendor=value");
        MapperTestKit.TestDatum datum = MapperTestKit.TestDatum.builder()
                .value("hello".getBytes())
                .systemMetadata(systemMetadata)
                .build();

        TracingMapFunction function = new TracingMapFunction();
        MessageList result = function.processMessage(new String[]{"key-1"}, datum);

        List<Message> messages = result.getMessages();
        Assertions.assertEquals(1, messages.size());
        Assertions.assertEquals("hello", new String(messages.get(0).getValue()));
        Assertions.assertEquals("key-1", messages.get(0).getKeys()[0]);
    }

    private static SystemMetadata tracingSystemMetadata(String traceparent, String tracestate) {
        MetadataOuterClass.KeyValueGroup.Builder groupBuilder =
                MetadataOuterClass.KeyValueGroup.newBuilder()
                        .putKeyValue("traceparent", ByteString.copyFromUtf8(traceparent));
        if (tracestate != null) {
            groupBuilder.putKeyValue("tracestate", ByteString.copyFromUtf8(tracestate));
        }

        MetadataOuterClass.Metadata protoMetadata = MetadataOuterClass.Metadata.newBuilder()
                .putSysMetadata("tracing_udf", groupBuilder.build())
                .build();

        return new SystemMetadata(protoMetadata);
    }
}
