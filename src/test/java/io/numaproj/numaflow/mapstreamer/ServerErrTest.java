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
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.map.v1.MapGrpc;
import io.numaproj.numaflow.map.v1.MapOuterClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ServerErrTest {

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private Service server;
    private ManagedChannel inProcessChannel;

    @Before
    public void setUp() throws Exception {
        ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    io.grpc.Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                final var context = Context.current();
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
                        // Translate it to INTERNAL status.
                        var status = Status.INTERNAL.withDescription(e.getMessage()).withCause(e);
                        var newStatus = Status.fromThrowable(status.asException());
                        serverCall.close(newStatus, headers);
                    }
                };
            }
        };

        String serverName = InProcessServerBuilder.generateName();

        CompletableFuture<Void> shutdownSignal = new CompletableFuture<>();

        server = new Service(new TestMapStreamerErr(), shutdownSignal);

        grpcCleanup.register(InProcessServerBuilder
                .forName(serverName)
                .directExecutor()
                .addService(server)
                .intercept(interceptor)
                .build()
                .start());

        inProcessChannel = grpcCleanup.register(InProcessChannelBuilder
                .forName(serverName)
                .directExecutor()
                .build());
    }

    @After
    public void tearDown() {
        inProcessChannel.shutdownNow();
    }

    @Test
    public void testMapStreamerFailure() {
        MapOuterClass.MapRequest handshakeRequest = MapOuterClass.MapRequest
                .newBuilder()
                .setHandshake(MapOuterClass.Handshake.newBuilder().setSot(true))
                .build();

        ByteString inValue = ByteString.copyFromUtf8("invalue");
        MapOuterClass.MapRequest mapRequest = MapOuterClass.MapRequest
                .newBuilder()
                .setRequest(MapOuterClass.MapRequest.Request.newBuilder()
                        .addAllKeys(List.of("test-map-key")).setValue(inValue).build())
                .build();

        MapStreamOutputStreamObserver responseObserver = new MapStreamOutputStreamObserver(2);

        var stub = MapGrpc.newStub(inProcessChannel);
        var requestObserver = stub.mapFn(responseObserver);

        requestObserver.onNext(handshakeRequest);
        requestObserver.onNext(mapRequest);

        try {
            responseObserver.done.get();
            fail("Expected exception not thrown");
        } catch (Exception e) {
            assertEquals(
                    "io.grpc.StatusRuntimeException: INTERNAL: unknown exception",
                    e.getMessage());
        }
    }

    private static class TestMapStreamerErr extends MapStreamer {
        @Override
        public void processMessage(String[] keys, Datum datum, OutputObserver outputObserver) {
            throw new RuntimeException("unknown exception");
        }
    }
}
