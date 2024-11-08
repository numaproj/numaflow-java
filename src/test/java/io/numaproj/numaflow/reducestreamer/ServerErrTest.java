package io.numaproj.numaflow.reducestreamer;

import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.reduce.v1.ReduceGrpc;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import io.numaproj.numaflow.reducestreamer.model.Datum;
import io.numaproj.numaflow.reducestreamer.model.OutputStreamObserver;
import io.numaproj.numaflow.reducestreamer.model.ReduceStreamer;
import io.numaproj.numaflow.reducestreamer.model.ReduceStreamerFactory;
import io.numaproj.numaflow.shared.GrpcServerUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static io.numaproj.numaflow.shared.GrpcServerUtils.WIN_END_KEY;
import static io.numaproj.numaflow.shared.GrpcServerUtils.WIN_START_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ServerErrTest {
    public static final Metadata.Key<String> DATUM_METADATA_WIN_START = Metadata.Key.of(
            WIN_START_KEY,
            Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> DATUM_METADATA_WIN_END = Metadata.Key.of(
            WIN_END_KEY,
            Metadata.ASCII_STRING_MARSHALLER);
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
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                final var context =
                        Context.current().withValues(
                                GrpcServerUtils.WINDOW_START_TIME,
                                headers.get(DATUM_METADATA_WIN_START),
                                GrpcServerUtils.WINDOW_END_TIME,
                                headers.get(DATUM_METADATA_WIN_END));
                return Contexts.interceptCall(context, call, headers, next);
            }
        };

        String serverName = InProcessServerBuilder.generateName();

        GRPCConfig grpcServerConfig = GRPCConfig.newBuilder()
                .maxMessageSize(io.numaproj.numaflow.reducestreamer.Constants.DEFAULT_MESSAGE_SIZE)
                .socketPath(io.numaproj.numaflow.reducestreamer.Constants.DEFAULT_SOCKET_PATH)
                .infoFilePath("/tmp/numaflow-test-server-info)")
                .build();

        server = new Server(
                grpcServerConfig,
                new ReduceStreamerErrTestFactory(),
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
    public void given_reducerThrows_when_serverRuns_then_outputStreamContainsThrowable() {
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of(WIN_START_KEY, Metadata.ASCII_STRING_MARSHALLER), "60000");
        metadata.put(Metadata.Key.of(WIN_END_KEY, Metadata.ASCII_STRING_MARSHALLER), "120000");

        // create an output stream observer
        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();
        // we need to maintain a reference to any exceptions thrown inside the thread, otherwise even if the assertion failed in the thread,
        // the test can still succeed.
        AtomicReference<Throwable> exceptionInThread = new AtomicReference<>();

        Thread t = new Thread(() -> {
            while (outputStreamObserver.t == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    exceptionInThread.set(e);
                }
            }
            try {
                assertEquals(
                        "UNKNOWN: java.lang.RuntimeException: unknown exception",
                        outputStreamObserver.t.getMessage());
            } catch (Throwable e) {
                exceptionInThread.set(e);
            }
        });
        t.start();

        StreamObserver<ReduceOuterClass.ReduceRequest> inputStreamObserver = ReduceGrpc
                .newStub(inProcessChannel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                .reduceFn(outputStreamObserver);

        for (int i = 1; i <= 10; i++) {
            ReduceOuterClass.ReduceRequest reduceRequest = ReduceOuterClass.ReduceRequest
                    .newBuilder()
                    .setPayload(ReduceOuterClass.ReduceRequest.Payload
                            .newBuilder()
                            .addKeys("reduce-key")
                            .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                            .build())
                    .build();
            inputStreamObserver.onNext(reduceRequest);
        }

        inputStreamObserver.onCompleted();

        try {
            t.join();
        } catch (InterruptedException e) {
            fail("Thread got interrupted before test assertion.");
        }
        // Fail the test if any exception caught in the thread
        if (exceptionInThread.get() != null) {
            fail("Assertion failed in the thread: " + exceptionInThread.get().getMessage());
        }
    }

    public static class ReduceStreamerErrTestFactory extends ReduceStreamerFactory<ServerErrTest.ReduceStreamerErrTestFactory.TestReduceStreamHandler> {
        @Override
        public TestReduceStreamHandler createReduceStreamer() {
            return new ServerErrTest.ReduceStreamerErrTestFactory.TestReduceStreamHandler();
        }

        public static class TestReduceStreamHandler extends ReduceStreamer {
            @Override
            public void processMessage(
                    String[] keys,
                    Datum datum,
                    OutputStreamObserver outputStream,
                    io.numaproj.numaflow.reducestreamer.model.Metadata md) {
                throw new RuntimeException("unknown exception");
            }

            @Override
            public void handleEndOfStream(
                    String[] keys,
                    OutputStreamObserver outputStreamObserver,
                    io.numaproj.numaflow.reducestreamer.model.Metadata md) {

            }
        }
    }
}
