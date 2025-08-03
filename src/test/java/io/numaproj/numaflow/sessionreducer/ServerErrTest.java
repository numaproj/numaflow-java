package io.numaproj.numaflow.sessionreducer;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
                new SessionReducerErrTestFactory(),
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
    public void given_actorThrows_when_serverRuns_then_outputStreamContainsThrowable() {
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
                // this test triggers a supervisor runtime exception by sending an OPEN request with 2 windows.
                // we are expecting the error message below.
                assertTrue(outputStreamObserver.t.getMessage().contains("expected exactly one window"));
            } catch (Throwable e) {
                exceptionInThread.set(e);
            }
        });
        t.start();

        StreamObserver<Sessionreduce.SessionReduceRequest> inputStreamObserver = SessionReduceGrpc
                .newStub(inProcessChannel)
                .sessionReduceFn(outputStreamObserver);

        List<String> testKey = List.of("test-key");
        // an open request with two windows is invalid
        Sessionreduce.SessionReduceRequest openRequest = Sessionreduce.SessionReduceRequest
                .newBuilder()
                .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                        .newBuilder()
                        .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.OPEN_VALUE)
                        .addAllKeyedWindows(List.of(
                                Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(testKey)
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(6000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(7000).build())
                                        .setSlot("test-slot").build(),
                                Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(testKey)
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(8000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(9000).build())
                                        .setSlot("test-slot").build()))
                        .build())
                .setPayload(Sessionreduce.SessionReduceRequest.Payload
                        .newBuilder()
                        .addAllKeys(testKey)
                        .setValue(ByteString.copyFromUtf8(String.valueOf(1)))
                        .build())
                .build();
        inputStreamObserver.onNext(openRequest);

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
                assertTrue(outputStreamObserver.t.getMessage().contains("UDF_EXECUTION_ERROR"));
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
