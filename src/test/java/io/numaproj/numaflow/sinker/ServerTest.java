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

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Slf4j
@RunWith(JUnit4.class)
public class ServerTest {
    private final static String processedIdSuffix = "-id-processed";
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private Server server;
    private ManagedChannel inProcessChannel;

    @Before
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        GRPCConfig grpcServerConfig = new GRPCConfig(SinkConstants.DEFAULT_MESSAGE_SIZE);
        grpcServerConfig.setInfoFilePath("/tmp/numaflow-test-server-info");
        server = new Server(
                new TestSinkFn(),
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
    public void sinkerSuccess() throws InterruptedException {
        //create an output stream observer
        SinkOutputStreamObserver outputStreamObserver = new SinkOutputStreamObserver();

        StreamObserver<SinkOuterClass.SinkRequest> inputStreamObserver = SinkGrpc
                .newStub(inProcessChannel)
                .sinkFn(outputStreamObserver);

        String actualId = "sink_test_id";
        String expectedId = actualId + processedIdSuffix;

        for (int i = 1; i <= 100; i++) {
            String[] keys;
            if (i < 100) {
                keys = new String[]{"valid-key"};
            } else {
                keys = new String[]{"invalid-key"};
            }
            SinkOuterClass.SinkRequest sinkRequest = SinkOuterClass.SinkRequest.newBuilder()
                    .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                    .setId(actualId)
                    .addAllKeys(List.of(keys))
                    .build();
            inputStreamObserver.onNext(sinkRequest);
        }

        inputStreamObserver.onCompleted();

        while(!outputStreamObserver.completed.get());
        SinkOuterClass.SinkResponse responseList = outputStreamObserver.getSinkResponse();
        assertEquals(100, responseList.getResultsCount());
        responseList.getResultsList().forEach((response -> {
            assertEquals(response.getId(), expectedId);
        }));

        assertEquals(
                responseList.getResults(responseList.getResultsCount() - 1).getErrMsg(),
                "error message");
    }

    @Slf4j
    private static class TestSinkFn extends Sinker {

        @Override
        public Response processMessage(Datum datum) {
            ResponseList.ResponseListBuilder builder = ResponseList.newBuilder();
            if (Arrays.equals(datum.getKeys(), new String[]{"invalid-key"})) {
                return Response.responseFailure(
                        datum.getId() + processedIdSuffix,
                        "error message");
            }
            log.info(new String(datum.getValue()));
            return Response.responseOK(datum.getId() + processedIdSuffix);
        }
    }
}
