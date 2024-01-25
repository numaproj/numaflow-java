package io.numaproj.numaflow.sessionreducer;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.sessionreduce.v1.SessionReduceGrpc;
import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;
import io.numaproj.numaflow.sessionreducer.model.Datum;
import io.numaproj.numaflow.sessionreducer.model.OutputStreamObserver;
import io.numaproj.numaflow.sessionreducer.model.SessionReducer;
import io.numaproj.numaflow.sessionreducer.model.SessionReducerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

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
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                final var context = Context.current();
                return Contexts.interceptCall(context, call, headers, next);
            }
        };

        String serverName = InProcessServerBuilder.generateName();

        GRPCConfig grpcServerConfig = GRPCConfig.newBuilder()
                .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
                .socketPath(Constants.DEFAULT_SOCKET_PATH)
                .infoFilePath("/tmp/numaflow-test-server-info)")
                .build();

        server = new Server(
                new SessionReducerErrTestFactory(),
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
    public void given_sessionReducerThrows_when_serverRuns_then_outputStreamContainsThrowable() {
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

        StreamObserver<Sessionreduce.SessionReduceRequest> inputStreamObserver = SessionReduceGrpc
                .newStub(inProcessChannel)
                .sessionReduceFn(outputStreamObserver);

        List<String> testKeys = List.of("reduce-key");
        for (int i = 1; i <= 10; i++) {
            Sessionreduce.SessionReduceRequest reduceRequest = Sessionreduce.SessionReduceRequest
                    .newBuilder()
                    .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                            .newBuilder()
                            .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.APPEND_VALUE)
                            .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                    .addAllKeys(testKeys)
                                    .setStart(Timestamp
                                            .newBuilder().setSeconds(6000).build())
                                    .setEnd(Timestamp.newBuilder().setSeconds(7000).build())
                                    .setSlot("test-slot").build()))
                            .build())
                    .setPayload(Sessionreduce.SessionReduceRequest.Payload
                            .newBuilder()
                            .addAllKeys(testKeys)
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

    public static class SessionReducerErrTestFactory extends SessionReducerFactory<SessionReducerErrTestFactory.TestSessionReducerHandler> {
        @Override
        public TestSessionReducerHandler createSessionReducer() {
            return new TestSessionReducerHandler();
        }

        public static class TestSessionReducerHandler extends SessionReducer {
            @Override
            public void processMessage(
                    String[] keys,
                    Datum datum,
                    OutputStreamObserver outputStream) {
                throw new RuntimeException("unknown exception");
            }

            @Override
            public void handleEndOfStream(
                    String[] keys,
                    OutputStreamObserver outputStreamObserver) {

            }

            @Override
            public byte[] accumulator() {
                return new byte[0];
            }

            @Override
            public void mergeAccumulator(byte[] accumulator) {

            }
        }
    }
}
