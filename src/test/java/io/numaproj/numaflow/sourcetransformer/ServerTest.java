package io.numaproj.numaflow.sourcetransformer;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.shared.Constants;
import io.numaproj.numaflow.sourcetransformer.v1.SourceTransformGrpc;
import io.numaproj.numaflow.sourcetransformer.v1.Sourcetransformer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ServerTest {
    private final static String PROCESSED_KEY_SUFFIX = "-key-processed";
    private final static String PROCESSED_VALUE_SUFFIX = "-value-processed";
    private final static Instant TEST_EVENT_TIME = Instant.MIN;

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private Server server;
    private ManagedChannel inProcessChannel;

    @Before
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();

        GRPCConfig grpcServerConfig = new GRPCConfig(Constants.DEFAULT_MESSAGE_SIZE);
        grpcServerConfig.setInfoFilePath("/tmp/numaflow-test-server-info");
        server = new Server( new TestSourceTransformer(),
                grpcServerConfig);

        server.setServerBuilder(InProcessServerBuilder.forName(serverName)
                .directExecutor());

        server.start();

        inProcessChannel = grpcCleanup.register(InProcessChannelBuilder
                .forName(serverName)
                .directExecutor()
                .build());
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void TestSourceTransform() {
        ByteString inValue = ByteString.copyFromUtf8("invalue");
        Sourcetransformer.SourceTransformRequest request = Sourcetransformer.SourceTransformRequest
                .newBuilder()
                .addKeys("test-st-key")
                .setValue(inValue)
                .build();

        String[] expectedKey = new String[]{"test-st-key" + PROCESSED_KEY_SUFFIX};
        String[] expectedTags = new String[]{"test-tag"};
        ByteString expectedValue = ByteString.copyFromUtf8("invalue" + PROCESSED_VALUE_SUFFIX);

        var stub = SourceTransformGrpc.newBlockingStub(inProcessChannel);
        var actualDatumList = stub
                .sourceTransformFn(request);

        assertEquals(1, actualDatumList.getResultsCount());
        assertEquals(
                        com.google.protobuf.Timestamp.newBuilder()
                                .setSeconds(TEST_EVENT_TIME.getEpochSecond())
                                .setNanos(TEST_EVENT_TIME.getNano()).build(),
                actualDatumList.getResults(0).getEventTime());
        assertEquals(
                expectedKey,
                actualDatumList.getResults(0).getKeysList().toArray(new String[0]));
        assertEquals(expectedValue, actualDatumList.getResults(0).getValue());
        assertEquals(
                expectedTags,
                actualDatumList.getResults(0).getTagsList().toArray(new String[0]));
    }

    private static class TestSourceTransformer extends SourceTransformer {
        @Override
        public MessageList processMessage(String[] keys, Datum datum) {
            String[] updatedKeys = Arrays
                    .stream(keys)
                    .map(c -> c + PROCESSED_KEY_SUFFIX)
                    .toArray(String[]::new);
            return MessageList
                    .newBuilder()
                    .addMessage(new Message(
                            (new String(datum.getValue())
                                    + PROCESSED_VALUE_SUFFIX).getBytes(),
                            TEST_EVENT_TIME,
                            updatedKeys,
                            new String[]{"test-tag"}))
                    .build();
        }
    }
}
