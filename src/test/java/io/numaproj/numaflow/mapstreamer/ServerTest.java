package io.numaproj.numaflow.mapstreamer;

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
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

        server = new Server(
                grpcServerConfig,
                new TestMapStreamFn(),
                null,
                serverName);

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
    public void TestMapStreamer() {
        MapOuterClass.MapRequest handshakeRequest = MapOuterClass.MapRequest
                .newBuilder()
                .setHandshake(MapOuterClass.Handshake.newBuilder().setSot(true))
                .build();

        ByteString inValue = ByteString.copyFromUtf8("invalue");
        MapOuterClass.MapRequest request = MapOuterClass.MapRequest
                .newBuilder()
                .setRequest(MapOuterClass.MapRequest.Request.newBuilder()
                        .setValue(inValue)
                        .addKeys("test-map-stream-key")).build();

        String[] expectedKeys = new String[]{"test-map-stream-key" + PROCESSED_KEY_SUFFIX};
        String[] expectedTags = new String[]{"test-tag"};
        ByteString expectedValue = ByteString.copyFromUtf8("invalue" + PROCESSED_VALUE_SUFFIX);

        MapStreamOutputStreamObserver mapStreamOutputStreamObserver = new MapStreamOutputStreamObserver(
                12);
        var stub = MapGrpc.newStub(inProcessChannel);

        var requestStreamObserver = stub
                .mapFn(mapStreamOutputStreamObserver);

        requestStreamObserver.onNext(handshakeRequest);
        requestStreamObserver.onNext(request);
        requestStreamObserver.onCompleted();

        try {
            mapStreamOutputStreamObserver.done.get();
        } catch (InterruptedException | ExecutionException e) {
            fail("exception thrown");
        }
        System.out.println("got responses");
        List<MapOuterClass.MapResponse> messages = mapStreamOutputStreamObserver.getMapResponses();
        // 10 responses + 1 handshake response + 1 eot response
        assertEquals(12, messages.size());

        // first is handshake
        assertTrue(messages.get(0).hasHandshake());

        messages = messages.subList(1, messages.size());
        for (MapOuterClass.MapResponse response : messages) {
            if (response.hasStatus()) {
                assertTrue(response.getStatus().getEot());
                continue;
            }
            assertEquals(expectedValue, response.getResults(0).getValue());
            assertEquals(expectedKeys[0], response.getResults(0).getKeys(0));
        }
    }

    private static class TestMapStreamFn extends MapStreamer {
        @Override
        public void processMessage(String[] keys, Datum datum, OutputObserver outputObserver) {
            String[] updatedKeys = Arrays
                    .stream(keys)
                    .map(c -> c + PROCESSED_KEY_SUFFIX)
                    .toArray(String[]::new);
            for (int i = 0; i < 10; i++) {
                Message msg = new Message(
                        (new String(datum.getValue())
                                + PROCESSED_VALUE_SUFFIX).getBytes(),
                        updatedKeys,
                        new String[]{"test-tag"});
                outputObserver.send(msg);
            }
        }
    }
}
