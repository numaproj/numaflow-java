package io.numaproj.numaflow.servingstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ServerErrTest {

    @org.junit.Rule
    public final io.grpc.testing.GrpcCleanupRule grpcCleanup = new io.grpc.testing.GrpcCleanupRule();
    private Server server;
    private io.grpc.ManagedChannel inProcessChannel;

    @org.junit.Before
    public void setUp() throws Exception {
        String serverName = io.grpc.inprocess.InProcessServerBuilder.generateName();

        GRPCConfig grpcServerConfig = GRPCConfig.newBuilder()
                .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
                .socketPath(Constants.DEFAULT_SOCKET_PATH)
                .infoFilePath("/tmp/numaflow-test-server-info)")
                .build();

        server = new Server(
                grpcServerConfig,
                new ServerErrTest.TestServingStorer(),
                null,
                serverName);

        server.start();

        inProcessChannel = grpcCleanup.register(io.grpc.inprocess.InProcessChannelBuilder
                .forName(serverName)
                .directExecutor()
                .build());
    }

    @org.junit.After
    public void tearDown() throws Exception {
        server.stop();
    }

    @org.junit.Test
    public void testServingStorePutGetSuccess() {
        io.numaproj.numaflow.serving.v1.ServingStoreGrpc.ServingStoreBlockingStub stub = io.numaproj.numaflow.serving.v1.ServingStoreGrpc.newBlockingStub(inProcessChannel);

        try {
            stub.put(io.numaproj.numaflow.serving.v1.Store.PutRequest.newBuilder()
                    .setId("test-id")
                    .addPayloads(io.numaproj.numaflow.serving.v1.Store.Payload.newBuilder()
                            .setOrigin("test-origin")
                            .setValue(com.google.protobuf.ByteString.copyFrom("test-value".getBytes()))
                            .build())
                    .build());
            fail("Expected an exception to be thrown");
        } catch (Exception e) {
            assertEquals("INTERNAL: UDF_EXECUTION_ERROR(servingstore): unknown exception", e.getMessage());
        }
    }

    private static class TestServingStorer extends ServingStorer {

        private final java.util.Map<String, java.util.List<Payload>> store = new java.util.HashMap<>();

        @Override
        public void put(io.numaproj.numaflow.servingstore.PutDatum putDatum) {
            throw new RuntimeException("unknown exception");
        }

        @Override
        public io.numaproj.numaflow.servingstore.StoredResult get(io.numaproj.numaflow.servingstore.GetDatum getDatum) {
            java.util.List<Payload> payloads = store.getOrDefault(getDatum.ID(), java.util.Collections.emptyList());
            return new StoredResult(getDatum.ID(), payloads);
        }
    }
}
