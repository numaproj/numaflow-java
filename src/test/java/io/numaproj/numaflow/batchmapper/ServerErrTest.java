package io.numaproj.numaflow.batchmapper;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.batchmap.v1.BatchMapGrpc;
import io.numaproj.numaflow.batchmap.v1.Batchmap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.UUID;
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

        String serverName = InProcessServerBuilder.generateName();

        GRPCConfig grpcServerConfig = GRPCConfig.newBuilder()
                .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
                .socketPath(Constants.DEFAULT_SOCKET_PATH)
                .infoFilePath("/tmp/numaflow-test-server-info)")
                .build();

        server = new Server(
                new TestMapFn(),
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
    public void testErrorFromUDF() {

        BatchMapOutputStreamObserver outputStreamObserver = new BatchMapOutputStreamObserver();
        StreamObserver<Batchmap.BatchMapRequest> inputStreamObserver = BatchMapGrpc
                .newStub(inProcessChannel)
                .batchMapFn(outputStreamObserver);

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
        String message = "message";
        Batchmap.BatchMapRequest request = Batchmap.BatchMapRequest.newBuilder()
                .setValue(ByteString.copyFromUtf8(message))
                .addKeys("exception")
                .setId(UUID.randomUUID().toString())
                .build();
        inputStreamObserver.onNext(request);
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

    private static class TestMapFn extends BatchMapper {

        @Override
        public BatchResponses processMessage(DatumIterator datumStream) {
            throw new RuntimeException("unknown exception");
        }
    }
}
