package io.numaproj.numaflow.sourcetransformer;

import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCallListener;
import io.grpc.ManagedChannel;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.sourcetransformer.v1.SourceTransformGrpc;
import io.numaproj.numaflow.sourcetransformer.v1.Sourcetransformer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ServerErrTest {

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private Server server;
    private ManagedChannel inProcessChannel;

    @Before
    public void setUp() throws Exception {
        ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    io.grpc.Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {

                final var context =
                        Context.current();
                ServerCall.Listener<ReqT> listener = Contexts.interceptCall(
                        context,
                        call,
                        headers,
                        next);
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(
                        listener) {
                    @Override
                    public void onHalfClose() {
                        try {
                            super.onHalfClose();
                        } catch (RuntimeException ex) {
                            handleException(ex, call, headers);
                            throw ex;
                        }
                    }

                    private void handleException(
                            RuntimeException e,
                            ServerCall<ReqT, RespT> serverCall,
                            io.grpc.Metadata headers) {
                        // Currently, we only have application level exceptions.
                        // Translate it to UNKNOWN status.
                        var status = Status.UNKNOWN.withDescription(e.getMessage()).withCause(e);
                        var newStatus = Status.fromThrowable(status.asException());
                        serverCall.close(newStatus, headers);
                    }
                };
            }
        };

        String serverName = InProcessServerBuilder.generateName();

        GRPCConfig grpcServerConfig = GRPCConfig.newBuilder()
                .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
                .socketPath(Constants.DEFAULT_SOCKET_PATH)
                .infoFilePath("/tmp/numaflow-test-server-info)")
                .build();

        server = new Server(
                new SourceTransformerTestErr(),
                grpcServerConfig);

        server.setServerBuilder(InProcessServerBuilder.forName(serverName)
                .intercept(interceptor)
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
    public void TestSourceTransformErr() {
        ByteString inValue = ByteString.copyFromUtf8("invalue");
        Sourcetransformer.SourceTransformRequest request = Sourcetransformer.SourceTransformRequest
                .newBuilder()
                .addKeys("test-sst-key")
                .setValue(inValue)
                .build();

        var stub = SourceTransformGrpc.newBlockingStub(inProcessChannel);
        try {
            stub.sourceTransformFn(request);
        } catch (Exception e) {
            assertEquals("UNKNOWN: unknown exception", e.getMessage());
        }
    }

    private static class SourceTransformerTestErr extends SourceTransformer {
        @Override
        public MessageList processMessage(String[] keys, Datum datum) {
            throw new RuntimeException("unknown exception");
        }
    }
}
