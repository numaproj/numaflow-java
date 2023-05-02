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
import io.numaproj.numaflow.sink.types.ResponseList;
import io.numaproj.numaflow.sink.v1.Udsink;
import io.numaproj.numaflow.sink.v1.UserDefinedSinkGrpc;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class SinkServerTest {
    private static final Logger logger = Logger.getLogger(SinkServerTest.class.getName());
    private final static String processedIdSuffix = "-id-processed";
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
        server.registerSinker(new TestSinkFn()).start();
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

        StreamObserver<Udsink.DatumRequest> inputStreamObserver = UserDefinedSinkGrpc
                .newStub(inProcessChannel)
                .sinkFn(outputStreamObserver);

        String actualId = "sink_test_id";
        String expectedId = actualId + processedIdSuffix;

        for (int i = 1; i <= 10; i++) {
            String[] keys;
            if (i < 10) {
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

        while(!outputStreamObserver.completed.get());
        Udsink.ResponseList responseList = outputStreamObserver.getResultDatum();
        assertEquals(10, responseList.getResponsesCount());
        responseList.getResponsesList().forEach((response -> {
            assertEquals(response.getId(), expectedId);
        }));

        assertEquals(
                responseList.getResponses(responseList.getResponsesCount() - 1).getErrMsg(),
                "error message");
    }

    private static class TestSinkFn extends SinkHandler {

        @Override
        public Response processMessage(Datum datum) {
            ResponseList.ResponseListBuilder builder = ResponseList.newBuilder();
            if (Arrays.equals(datum.getKeys(), new String[]{"invalid-key"})) {
                return Response.responseFailure(
                        datum.getId() + processedIdSuffix,
                        "error message");
            }

            logger.info(Arrays.toString(datum.getValue()));
            return Response.responseOK(datum.getId() + processedIdSuffix);
        }
    }
}
