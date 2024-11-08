package io.numaproj.numaflow.sourcer;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.source.v1.SourceGrpc;
import io.numaproj.numaflow.source.v1.SourceOuterClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
                new TestSourcerErr(),
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
    public void TestSourcerErr() {
        var stub = SourceGrpc.newStub(inProcessChannel);

        // Test readFn, source has 10 messages
        // we read 5 messages, ack them, then read another 5 messages
        SourceOuterClass.ReadRequest request = SourceOuterClass.ReadRequest.newBuilder()
                .setRequest(SourceOuterClass.ReadRequest.Request
                        .newBuilder()
                        .setNumRecords(5)
                        .setTimeoutInMs(1000)
                        .build())
                .build();

        StreamObserver<SourceOuterClass.ReadRequest> readRequestObserver = stub.readFn(new StreamObserver<SourceOuterClass.ReadResponse>() {
            @Override
            public void onNext(SourceOuterClass.ReadResponse readResponse) {
                // Handle onNext
            }

            @Override
            public void onError(Throwable throwable) {
                assertEquals("UNKNOWN: Application error processing RPC", throwable.getMessage());
            }

            @Override
            public void onCompleted() {
                // Handle onCompleted
            }
        });

        readRequestObserver.onNext(request);
        readRequestObserver.onCompleted();
    }

    @Test
    public void sourceWithoutAckHandshake() {
        // Create an output stream observer
        AckOutputStreamObserver outputStreamObserver = new AckOutputStreamObserver();

        StreamObserver<SourceOuterClass.AckRequest> inputStreamObserver = SourceGrpc
                .newStub(inProcessChannel)
                .ackFn(outputStreamObserver);

        // Send a request without sending a handshake request
        SourceOuterClass.AckRequest request = SourceOuterClass.AckRequest.newBuilder()
                .setRequest(SourceOuterClass.AckRequest.Request.newBuilder()
                        .build())
                .build();
        inputStreamObserver.onNext(request);

        // Wait for the server to process the request
        while (!outputStreamObserver.completed.get()) ;

        // Check if an error was received
        assertNotNull(outputStreamObserver.t);
        assertEquals(
                "INVALID_ARGUMENT: Handshake request not received",
                outputStreamObserver.t.getMessage());
    }

    @Test
    public void sourceWithoutReadHandshake() {
        // Create an output stream observer
        ReadOutputStreamObserver outputStreamObserver = new ReadOutputStreamObserver();

        StreamObserver<SourceOuterClass.ReadRequest> inputStreamObserver = SourceGrpc
                .newStub(inProcessChannel)
                .readFn(outputStreamObserver);

        // Send a request without sending a handshake request
        SourceOuterClass.ReadRequest request = SourceOuterClass.ReadRequest.newBuilder()
                .setRequest(SourceOuterClass.ReadRequest.Request.newBuilder()
                        .build())
                .build();
        inputStreamObserver.onNext(request);

        // Wait for the server to process the request
        while (!outputStreamObserver.completed.get()) ;

        // Check if an error was received
        assertNotNull(outputStreamObserver.t);
        assertEquals(
                "INVALID_ARGUMENT: Handshake request not received",
                outputStreamObserver.t.getMessage());
    }

    private static class TestSourcerErr extends Sourcer {

        @Override
        public void read(ReadRequest request, OutputObserver observer) {
            throw new RuntimeException("unknown exception");
        }

        @Override
        public void ack(AckRequest request) {
            throw new RuntimeException("unknown exception");

        }

        @Override
        public List<Integer> getPartitions() {
            return Sourcer.defaultPartitions();
        }

        @Override
        public long getPending() {
            throw new RuntimeException("unknown exception");
        }
    }
}
