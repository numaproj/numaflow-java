package io.numaproj.numaflow.batchmapper;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.map.v1.MapGrpc;
import io.numaproj.numaflow.map.v1.MapOuterClass;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Slf4j
public class ServerTest {
    private final static String key = "key";

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
    public void testBatchMapHappyPath() {
        BatchMapOutputStreamObserver outputStreamObserver = new BatchMapOutputStreamObserver(11);
        StreamObserver<MapOuterClass.MapRequest> inputStreamObserver = MapGrpc
                .newStub(inProcessChannel)
                .mapFn(outputStreamObserver);

        MapOuterClass.MapRequest handshakeRequest = MapOuterClass.MapRequest
                .newBuilder()
                .setHandshake(MapOuterClass.Handshake.newBuilder().setSot(true))
                .build();

        inputStreamObserver.onNext(handshakeRequest);
        for (int i = 1; i <= 10; i++) {
            String uuid = Integer.toString(i);
            String message = i + "," + (i + 10);
            MapOuterClass.MapRequest request = MapOuterClass.MapRequest.newBuilder()
                    .setRequest(MapOuterClass.MapRequest.Request
                            .newBuilder()
                            .setValue(ByteString.copyFromUtf8(message))
                            .addKeys(key)
                            .build())
                    .setId(uuid)
                    .build();
            inputStreamObserver.onNext(request);
        }

        inputStreamObserver.onNext(MapOuterClass.MapRequest
                .newBuilder()
                .setStatus(MapOuterClass.MapRequest.Status.newBuilder().setEot(true))
                .build());
        inputStreamObserver.onCompleted();

        try {
            outputStreamObserver.done.get();
        } catch (InterruptedException | ExecutionException e) {
            fail("Error in getting done signal from the observer " + e.getMessage());
        }
        List<MapOuterClass.MapResponse> result = outputStreamObserver.getMapResponses();
        assertEquals(11, result.size());

        // first response is handshake
        assertTrue(result.get(0).hasHandshake());

        result = result.subList(1, result.size());
        for (int i = 0; i < 10; i++) {
            assertEquals(result.get(i).getId(), String.valueOf(i + 1));
        }
    }

    private static class TestMapFn extends BatchMapper {
        @Override
        public BatchResponses processMessage(DatumIterator datumStream) {
            BatchResponses batchResponses = new BatchResponses();
            while (true) {
                Datum datum;
                try {
                    datum = datumStream.next();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    continue;
                }
                if (datum == null) {
                    break;
                }
                String msg = new String(datum.getValue());
                String[] strs = msg.split(",");
                BatchResponse batchResponse = new BatchResponse(datum.getId());
                for (String str : strs) {
                    batchResponse.append(new Message(str.getBytes()));
                }
                batchResponses.append(batchResponse);
            }
            return batchResponses;
        }
    }
}
