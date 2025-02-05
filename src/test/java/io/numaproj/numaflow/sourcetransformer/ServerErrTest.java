package io.numaproj.numaflow.sourcetransformer;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.sourcetransformer.v1.SourceTransformGrpc;
import io.numaproj.numaflow.sourcetransformer.v1.Sourcetransformer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
                new SourceTransformerTestErr(),
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
    public void testSourceTransformerFailure() {
        Sourcetransformer.SourceTransformRequest handshakeRequest = Sourcetransformer.SourceTransformRequest
                .newBuilder()
                .setHandshake(Sourcetransformer.Handshake
                        .newBuilder()
                        .setSot(true)
                        .build())
                .build();

        ByteString inValue = ByteString.copyFromUtf8("invalue");
        Sourcetransformer.SourceTransformRequest.Request inDatum = Sourcetransformer.SourceTransformRequest.Request
                .newBuilder()
                .setValue(inValue)
                .addAllKeys(List.of("test-st-key"))
                .build();

        Sourcetransformer.SourceTransformRequest request = Sourcetransformer.SourceTransformRequest
                .newBuilder()
                .setRequest(inDatum)
                .build();

        TransformerOutputStreamObserver responseObserver = new TransformerOutputStreamObserver(2);

        var stub = SourceTransformGrpc.newStub(inProcessChannel);
        var requestObserver = stub.sourceTransformFn(responseObserver);

        requestObserver.onNext(handshakeRequest);
        requestObserver.onNext(request);

        try {
            responseObserver.done.get();
            fail("Expected exception not thrown");
        } catch (Exception e) {
            String expectedSubstring = "UDF_EXECUTION_ERROR(transformer)";
            String actualMessage = e.getMessage();
            assertNotNull("Error message should not be null", actualMessage);
            assertTrue("Expected substring '" + expectedSubstring + "' not found in error message: " + actualMessage,
                    actualMessage.contains(expectedSubstring));
        }
    }

    private static class SourceTransformerTestErr extends SourceTransformer {
        @Override
        public MessageList processMessage(String[] keys, Datum datum) {
            throw new RuntimeException("unknown exception");
        }
    }
}
