package io.numaproj.numaflow.sideinput;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.sideinput.v1.SideInputGrpc;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ServerTest {

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
                new TestSideInput(),
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
    public void TestSideInputRetriever() {
        var stub = SideInputGrpc.newBlockingStub(inProcessChannel);

        // First call should return the broadcast message
        var sideInputResponse = stub.retrieveSideInput(Empty.newBuilder().build());
        assertEquals("test-side-input", new String(sideInputResponse.getValue().toByteArray()));

        // Second call should return no broadcast message
        sideInputResponse = stub.retrieveSideInput(Empty.newBuilder().build());
        assertEquals(0, sideInputResponse.getValue().size());
    }

    private static class TestSideInput extends SideInputRetriever {
        int count = 0;

        @Override
        public Message retrieveSideInput() {
            count++;
            if (count > 1) {
                return Message.createNoBroadcastMessage();
            }
            return Message.createBroadcastMessage("test-side-input".getBytes());
        }
    }
}
