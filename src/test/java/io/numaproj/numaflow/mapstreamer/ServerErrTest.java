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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ServerErrTest {

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
                new TestMapStreamFnErr(),
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
    public void TestMapStreamerErr() {
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

        MapStreamOutputStreamObserver mapStreamOutputStreamObserver = new MapStreamOutputStreamObserver(
                2);
        var stub = MapGrpc.newStub(inProcessChannel);

        var requestStreamObserver = stub
                .mapFn(mapStreamOutputStreamObserver);
        requestStreamObserver.onNext(handshakeRequest);
        requestStreamObserver.onNext(request);

        try {
            mapStreamOutputStreamObserver.done.get();
            fail("Should have thrown an exception");
        } catch (Exception e) {
            assertEquals(
                    "io.grpc.StatusRuntimeException: INTERNAL: UDF_EXECUTION_ERROR(mapstream): unknown exception",
                    e.getMessage());
        }
    }

    private static class TestMapStreamFnErr extends MapStreamer {
        @Override
        public void processMessage(String[] keys, Datum datum, OutputObserver outputObserver) {
            throw new RuntimeException("unknown exception");
        }
    }
}
