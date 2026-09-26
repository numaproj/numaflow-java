package io.numaproj.numaflow.examples.sink.tracing;

import com.google.protobuf.ByteString;
import common.MetadataOuterClass;
import io.numaproj.numaflow.shared.SystemMetadata;
import io.numaproj.numaflow.sinker.Response;
import io.numaproj.numaflow.sinker.ResponseList;
import io.numaproj.numaflow.sinker.SinkerTestKit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TracingSinkTest {

    private static final String TRACEPARENT =
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @Test
    public void testTracingSink() {
        int datumCount = 10;
        TracingSink tracingSink = new TracingSink();
        SinkerTestKit.TestListIterator testListIterator = new SinkerTestKit.TestListIterator();
        for (int i = 0; i < datumCount; i++) {
            testListIterator.addDatum(
                    SinkerTestKit.TestDatum.builder()
                            .id("id-" + i)
                            .value(("value-" + i).getBytes())
                            .build());
        }

        ResponseList responseList = tracingSink.processMessages(testListIterator);
        Assertions.assertEquals(datumCount, responseList.getResponses().size());
        for (Response response : responseList.getResponses()) {
            Assertions.assertTrue(response.getSuccess());
        }
    }

    @Test
    public void testTracingSinkWithTracingMetadata() {
        TracingSink tracingSink = new TracingSink();
        SinkerTestKit.TestListIterator testListIterator = new SinkerTestKit.TestListIterator();
        testListIterator.addDatum(
                SinkerTestKit.TestDatum.builder()
                        .id("traced-id")
                        .value("traced-value".getBytes())
                        .systemMetadata(tracingSystemMetadata(TRACEPARENT, "vendor=value"))
                        .build());

        ResponseList responseList = tracingSink.processMessages(testListIterator);
        Assertions.assertEquals(1, responseList.getResponses().size());
        Assertions.assertTrue(responseList.getResponses().get(0).getSuccess());
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
