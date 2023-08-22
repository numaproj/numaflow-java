package io.numaproj.numaflow.mapstreamer;

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
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.mapstream.v1.MapStreamGrpc;
import io.numaproj.numaflow.mapstream.v1.Mapstream;
import io.numaproj.numaflow.shared.Constants;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

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
                        Context.current().withValues(
                                Constants.WINDOW_START_TIME,
                                headers.get(Constants.DATUM_METADATA_WIN_START),
                                Constants.WINDOW_END_TIME,
                                headers.get(Constants.DATUM_METADATA_WIN_END));
                ServerCall.Listener<ReqT> listener = Contexts.interceptCall(context, call, headers, next);
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
                    @Override
                    public void onHalfClose() {
                        try {
                            super.onHalfClose();
                        } catch (RuntimeException ex) {
                            handleException(ex, call, headers);
                            throw ex;
                        }
                    }
                    private void handleException(RuntimeException e, ServerCall<ReqT, RespT> serverCall, io.grpc.Metadata headers) {
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

        GRPCConfig grpcServerConfig = new GRPCConfig(Constants.DEFAULT_MESSAGE_SIZE);
        grpcServerConfig.setInfoFilePath("/tmp/numaflow-test-server-info");
        server = new Server(new TestMapStreamFnErr(),
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
    public void TestMapStreamerErr() {
        ByteString inValue = ByteString.copyFromUtf8("invalue");
        Mapstream.MapStreamRequest request = Mapstream.MapStreamRequest
                .newBuilder()
                .addAllKeys(List.of("test-map-stream-key"))
                .setValue(inValue)
                .build();

        var stub = MapStreamGrpc.newBlockingStub(inProcessChannel);
        try {
            stub.mapStreamFn(request);
        } catch (Exception e) {
            assertEquals("UNKNOWN: unknown exception", e.getMessage());
        }
    }

    private static class TestMapStreamFnErr extends MapStreamer {
        @Override
        public void processMessage(String[] keys, Datum datum, StreamObserver<Mapstream.MapStreamResponse> streamObserver) {
            throw new RuntimeException("unknown exception");
        }
    }
}
