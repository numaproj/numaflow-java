package io.numaproj.numaflow.mapper;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.map.v1.MapGrpc;
import io.numaproj.numaflow.map.v1.MapOuterClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ServerTest {
    private final static String PROCESSED_KEY_SUFFIX = "-key-processed";
    private final static String PROCESSED_VALUE_SUFFIX = "-value-processed";

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private Server server;
    private ManagedChannel inProcessChannel;

    @Before
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();

        GRPCConfig grpcServerConfig = GRPCConfig.newBuilder()
                .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
                .socketPath(Constants.DEFAULT_SOCKET_PATH)
                .infoFilePath("/tmp/numaflow-test-server-info)")
                .build();

        server = new Server( new TestMapFn(),
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
    public void TestMapper() {
        ByteString inValue = ByteString.copyFromUtf8("invalue");
        MapOuterClass.MapRequest inDatum = MapOuterClass.MapRequest
                .newBuilder()
                .addAllKeys(List.of("test-map-key"))
                .setValue(inValue)
                .build();

        String[] expectedKeys = new String[]{"test-map-key" + PROCESSED_KEY_SUFFIX};
        String[] expectedTags = new String[]{"test-tag"};
        ByteString expectedValue = ByteString.copyFromUtf8("invalue" + PROCESSED_VALUE_SUFFIX);


        var stub = MapGrpc.newBlockingStub(inProcessChannel);
        var mapResponse = stub
                .mapFn(inDatum);

        assertEquals(1, mapResponse.getResultsCount());
        assertEquals(
                expectedKeys,
                mapResponse.getResults(0).getKeysList().toArray(new String[0]));
        assertEquals(expectedValue, mapResponse.getResults(0).getValue());
        assertEquals(
                expectedTags,
                mapResponse.getResults(0).getTagsList().toArray(new String[0]));
    }

    private static class TestMapFn extends Mapper {
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
                            updatedKeys,
                            new String[]{"test-tag"}))
                    .build();
        }
    }
}
