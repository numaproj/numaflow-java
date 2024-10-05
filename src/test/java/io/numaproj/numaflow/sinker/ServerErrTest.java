package io.numaproj.numaflow.sinker;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.sink.v1.SinkGrpc;
import io.numaproj.numaflow.sink.v1.SinkOuterClass;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@Slf4j
@RunWith(JUnit4.class)
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
                new TestSinkFnErr(),
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
    public void sinkerException() {
        //create an output stream observer
        SinkOutputStreamObserver outputStreamObserver = new SinkOutputStreamObserver();

        Thread t = new Thread(() -> {
            while (outputStreamObserver.t == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            assertEquals(
                    "UNKNOWN: java.lang.RuntimeException: unknown exception",
                    outputStreamObserver.t.getMessage());
        });
        t.start();

        StreamObserver<SinkOuterClass.SinkRequest> inputStreamObserver = SinkGrpc
                .newStub(inProcessChannel)
                .sinkFn(outputStreamObserver);
        String actualId = "sink_test_id";

        // send handshake request
        inputStreamObserver.onNext(SinkOuterClass.SinkRequest.newBuilder()
                .setHandshake(SinkOuterClass.Handshake.newBuilder().setSot(true).build())
                .build());

        for (int i = 1; i <= 100; i++) {
            String[] keys;
            if (i < 100) {
                keys = new String[]{"valid-key"};
            } else {
                keys = new String[]{"invalid-key"};
            }
            SinkOuterClass.SinkRequest.Request request = SinkOuterClass.SinkRequest.Request
                    .newBuilder()
                    .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                    .setId(actualId)
                    .addAllKeys(List.of(keys))
                    .build();
            inputStreamObserver.onNext(SinkOuterClass.SinkRequest
                    .newBuilder()
                    .setRequest(request)
                    .build());
        }

        // send eot message
        inputStreamObserver.onNext(SinkOuterClass.SinkRequest.newBuilder()
                .setStatus(SinkOuterClass.SinkRequest.Status.newBuilder().setEot(true)).build());

        inputStreamObserver.onCompleted();

        try {
            t.join();
        } catch (InterruptedException e) {
            fail("Thread interrupted");
        }
    }

    @Test
    public void sinkerNoHandshake() {
        // Create an output stream observer
        SinkOutputStreamObserver outputStreamObserver = new SinkOutputStreamObserver();

        StreamObserver<SinkOuterClass.SinkRequest> inputStreamObserver = SinkGrpc
                .newStub(inProcessChannel)
                .sinkFn(outputStreamObserver);

        // Send a request without sending a handshake request
        SinkOuterClass.SinkRequest request = SinkOuterClass.SinkRequest.newBuilder()
                .setRequest(SinkOuterClass.SinkRequest.Request.newBuilder()
                        .setValue(ByteString.copyFromUtf8("test"))
                        .setId("test_id")
                        .addKeys("test_key")
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

    @Slf4j
    private static class TestSinkFnErr extends Sinker {
        @Override
        public ResponseList processMessages(DatumIterator datumIterator) {
            throw new RuntimeException("unknown exception");
        }
    }
}
