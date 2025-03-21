package io.numaproj.numaflow.accumulator;

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
import io.numaproj.numaflow.accumulator.model.Accumulator;
import io.numaproj.numaflow.accumulator.model.AccumulatorFactory;
import io.numaproj.numaflow.accumulator.model.Datum;
import io.numaproj.numaflow.accumulator.model.OutputStreamObserver;
import io.numaproj.numaflow.accumulator.v1.AccumulatorGrpc;
import io.numaproj.numaflow.accumulator.v1.AccumulatorOuterClass;
import io.numaproj.numaflow.shared.ExceptionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
                grpcServerConfig,
                new TestAccumFactory(),
                interceptor,
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
    public void testAccumulatorFailure() {
        AccumulatorOuterClass.AccumulatorRequest openRequest = AccumulatorOuterClass.AccumulatorRequest
                .newBuilder()
                .setPayload(AccumulatorOuterClass.Payload
                        .newBuilder()
                        .setValue(ByteString.copyFromUtf8("test-payload"))
                        .addAllKeys(List.of("test-accumulator"))
                        .build())
                .setOperation(AccumulatorOuterClass.AccumulatorRequest.WindowOperation
                        .newBuilder()
                        .setEvent(AccumulatorOuterClass.AccumulatorRequest.WindowOperation.Event.OPEN)
                        .build()).build();

        AccumulatorStreamObserver responseObserver = new AccumulatorStreamObserver(4);

        var stub = AccumulatorGrpc.newStub(inProcessChannel);
        var requestObserver = stub.accumulateFn(responseObserver);

        requestObserver.onNext(openRequest);
        try {
            responseObserver.done.get();
            fail("Expected exception not thrown");
        } catch (Exception e) {
            assertEquals(
                    "io.grpc.StatusRuntimeException: INTERNAL: "
                            + ExceptionUtils.getExceptionErrorString()
                            + ": java.lang.RuntimeException: unknown exception",
                    e.getMessage());
        }
    }

    private static class TestAccumFn extends Accumulator {

        @Override
        public void processMessage(Datum datum, OutputStreamObserver outputStream) {
            throw new RuntimeException("unknown exception");
        }

        @Override
        public void handleEndOfStream(OutputStreamObserver outputStreamObserver) {
        }

    }

    private static class TestAccumFactory extends AccumulatorFactory<Accumulator> {
        @Override
        public Accumulator createAccumulator() {
            return new TestAccumFn();
        }

    }
}
