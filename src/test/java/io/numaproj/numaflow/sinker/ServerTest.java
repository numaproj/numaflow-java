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
import static org.junit.Assert.assertTrue;

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

        GRPCConfig grpcServerConfig = GRPCConfig.newBuilder()
                .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
                .socketPath(Constants.DEFAULT_SOCKET_PATH)
                .infoFilePath("/tmp/numaflow-test-server-info)")
                .build();

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
    public void sinkerSuccess() {
        //create an output stream observer
        SinkOutputStreamObserver outputStreamObserver = new SinkOutputStreamObserver();

        StreamObserver<SinkOuterClass.SinkRequest> inputStreamObserver = SinkGrpc
                .newStub(inProcessChannel)
                .sinkFn(outputStreamObserver);

        String actualId = "sink_test_id";
        String expectedId = actualId + processedIdSuffix;

        // Send a handshake request
        SinkOuterClass.SinkRequest handshakeRequest = SinkOuterClass.SinkRequest.newBuilder()
                .setHandshake(SinkOuterClass.Handshake.newBuilder().setSot(true).build())
                .build();
        inputStreamObserver.onNext(handshakeRequest);

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

            SinkOuterClass.SinkRequest sinkRequest = SinkOuterClass.SinkRequest.newBuilder()
                    .setRequest(request).build();
            inputStreamObserver.onNext(sinkRequest);

            // If it's the end of the batch, send an EOT message
            if (i % 10 == 0) {
                SinkOuterClass.SinkRequest eotRequest = SinkOuterClass.SinkRequest.newBuilder()
                        .setStatus(SinkOuterClass.SinkRequest.Status
                                .newBuilder()
                                .setEot(true)
                                .build())
                        .build();
                inputStreamObserver.onNext(eotRequest);
            }
        }

        inputStreamObserver.onCompleted();

        while (!outputStreamObserver.completed.get()) ;
        List<SinkOuterClass.SinkResponse> responseList = outputStreamObserver.getSinkResponse();
        assertEquals(101, responseList.size());
        // first response is the handshake response
        assertTrue(responseList.get(0).getHandshake().getSot());

        responseList = responseList.subList(1, responseList.size());
        responseList.forEach(response -> {
            assertEquals(response.getResult().getId(), expectedId);
            if (response.getResult().getStatus() == SinkOuterClass.Status.FAILURE) {
                assertEquals(response.getResult().getErrMsg(), "error message");
            }
        });
    }

    @Slf4j
    private static class TestSinkFn extends Sinker {
        @Override
        public ResponseList processMessages(DatumIterator datumIterator) {
            ResponseList.ResponseListBuilder builder = ResponseList.newBuilder();

            while (true) {
                Datum datum;
                try {
                    datum = datumIterator.next();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    continue;
                }
                if (datum == null) {
                    break;
                }
                if (Arrays.equals(datum.getKeys(), new String[]{"invalid-key"})) {
                    builder.addResponse(Response.responseFailure(
                            datum.getId() + processedIdSuffix,
                            "error message"));
                    continue;
                }
                log.info(new String(datum.getValue()));
                builder.addResponse(Response.responseOK(datum.getId() + processedIdSuffix));
            }

            return builder.build();
        }

    }
}
