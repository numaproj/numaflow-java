package io.numaproj.numaflow.sideinput;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.shared.Constants;
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

        GRPCConfig grpcServerConfig = new GRPCConfig(Constants.DEFAULT_MESSAGE_SIZE);
        server = new Server( new TestSideInput(),
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
    public void TestSideInputRetriever() {
        ByteString inValue = ByteString.copyFromUtf8("invalue");

        var stub = SideInputGrpc.newBlockingStub(inProcessChannel);
        var sideInputResponse = stub.retrieveSideInput(Empty.newBuilder().build());

        assertEquals("test-side-input", new String(sideInputResponse.getValue().toByteArray()));
    }

    private static class TestSideInput extends SideInputRetriever {
        @Override
        public Message retrieveSideInput() {
            return new Message("test-side-input".getBytes());
        }
    }
}
