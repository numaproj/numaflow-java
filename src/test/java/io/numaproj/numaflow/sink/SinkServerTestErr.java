package io.numaproj.numaflow.sink;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.sink.handler.SinkHandler;
import io.numaproj.numaflow.sink.interfaces.Datum;
import io.numaproj.numaflow.sink.types.Response;
import io.numaproj.numaflow.sink.v1.Udsink;
import io.numaproj.numaflow.sink.v1.UserDefinedSinkGrpc;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Slf4j
@RunWith(JUnit4.class)
public class SinkServerTestErr {
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private SinkServer server;
    private ManagedChannel inProcessChannel;

    @Before
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        SinkGRPCConfig grpcServerConfig = new SinkGRPCConfig(SinkConstants.DEFAULT_MESSAGE_SIZE);
        grpcServerConfig.setInfoFilePath("/tmp/numaflow-test-server-info");
        server = new SinkServer(
                InProcessServerBuilder.forName(serverName).directExecutor(),
                grpcServerConfig);
        server.registerSinker(new TestSinkFnErr()).start();
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
    public void sinkerSuccess() {
        //create an output stream observer
        SinkOutputStreamObserver outputStreamObserver = new SinkOutputStreamObserver();

        Thread t = new Thread(() -> {
            while (outputStreamObserver.t == null){
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            assertEquals("UNKNOWN: java.lang.RuntimeException: unknown exception", outputStreamObserver.t.getMessage());
        });
        t.start();

        StreamObserver<Udsink.DatumRequest> inputStreamObserver = UserDefinedSinkGrpc
                .newStub(inProcessChannel)
                .sinkFn(outputStreamObserver);
        String actualId = "sink_test_id";

        for (int i = 1; i <= 100; i++) {
            String[] keys;
            if (i < 100) {
                keys = new String[]{"valid-key"};
            } else {
                keys = new String[]{"invalid-key"};
            }
            Udsink.DatumRequest inputDatum = Udsink.DatumRequest.newBuilder()
                    .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                    .setId(actualId)
                    .addAllKeys(List.of(keys))
                    .build();
            inputStreamObserver.onNext(inputDatum);
        }

        inputStreamObserver.onCompleted();

        try {
            t.join();
        } catch (InterruptedException e) {
            fail("Thread interrupted");
        }
    }

    @Slf4j
    private static class TestSinkFnErr extends SinkHandler {

        @Override
        public Response processMessage(Datum datum) {
            throw new RuntimeException("unknown exception");
        }
    }
}
