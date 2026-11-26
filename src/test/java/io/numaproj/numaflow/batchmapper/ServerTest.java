package io.numaproj.numaflow.batchmapper;

import common.MetadataOuterClass;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.map.v1.MapGrpc;
import io.numaproj.numaflow.map.v1.MapOuterClass;
import io.numaproj.numaflow.shared.UserMetadata;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Slf4j
public class ServerTest {
    private final static String key = "key";

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private Server server;
    private ManagedChannel inProcessChannel;
    private TestMapFn testMapFn;

    @Before
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();

        GRPCConfig grpcServerConfig = GRPCConfig.newBuilder()
                .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
                .socketPath(Constants.DEFAULT_SOCKET_PATH)
                .infoFilePath("/tmp/numaflow-test-server-info)")
                .build();

        testMapFn = new TestMapFn();
        server = new Server(
                grpcServerConfig,
                testMapFn,
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
    public void testBatchMapHappyPath() {
        BatchMapOutputStreamObserver outputStreamObserver = new BatchMapOutputStreamObserver(12);
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
                .setStatus(MapOuterClass.TransmissionStatus.newBuilder().setEot(true))
                .build());
        inputStreamObserver.onCompleted();

        try {
            outputStreamObserver.done.get();
        } catch (InterruptedException | ExecutionException e) {
            fail("Error in getting done signal from the observer " + e.getMessage());
        }
        List<MapOuterClass.MapResponse> result = outputStreamObserver.getMapResponses();
        assertEquals(12, result.size());

        // first response is handshake
        assertTrue(result.get(0).hasHandshake());

        result = result.subList(1, result.size());
        for (int i = 0; i < 10; i++) {
            assertEquals(result.get(i).getId(), String.valueOf(i + 1));
            assertFalse(result.get(i).getResults(0).hasMetadata());
        }
    }

    @Test
    public void testBatchMapMetadata() {
        BatchMapOutputStreamObserver outputStreamObserver = new BatchMapOutputStreamObserver(3);
        StreamObserver<MapOuterClass.MapRequest> inputStreamObserver = MapGrpc
                .newStub(inProcessChannel)
                .mapFn(outputStreamObserver);

        inputStreamObserver.onNext(MapOuterClass.MapRequest.newBuilder()
                .setHandshake(MapOuterClass.Handshake.newBuilder().setSot(true))
                .build());

        MetadataOuterClass.Metadata metadata = MetadataOuterClass.Metadata.newBuilder()
                .putSysMetadata("tracing_udf", MetadataOuterClass.KeyValueGroup.newBuilder()
                        .putKeyValue("traceparent", ByteString.copyFromUtf8("00-trace-span-01"))
                        .build())
                .putUserMetadata("app", MetadataOuterClass.KeyValueGroup.newBuilder()
                        .putKeyValue("key", ByteString.copyFromUtf8("value"))
                        .build())
                .build();
        inputStreamObserver.onNext(MapOuterClass.MapRequest.newBuilder()
                .setId("metadata")
                .setRequest(MapOuterClass.MapRequest.Request.newBuilder()
                        .setValue(ByteString.copyFromUtf8("message"))
                        .setMetadata(metadata)
                        .build())
                .build());
        inputStreamObserver.onNext(MapOuterClass.MapRequest.newBuilder()
                .setStatus(MapOuterClass.TransmissionStatus.newBuilder().setEot(true))
                .build());
        inputStreamObserver.onCompleted();

        try {
            outputStreamObserver.done.get();
        } catch (InterruptedException | ExecutionException e) {
            fail("Error in getting done signal from the observer " + e.getMessage());
        }

        MapOuterClass.MapResponse response = outputStreamObserver.getMapResponses().get(1);
        assertEquals(
                ByteString.copyFromUtf8("value"),
                response.getResults(0).getMetadata()
                        .getUserMetadataOrThrow("app")
                        .getKeyValueOrThrow("key"));
        assertTrue(response.getResults(0).getMetadata().getSysMetadataMap().isEmpty());
        assertArrayEquals("00-trace-span-01".getBytes(), testMapFn.traceparent);
    }

    private static class TestMapFn extends BatchMapper {
        private volatile byte[] traceparent;

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
                traceparent = datum.getSystemMetadata().getValue("tracing_udf", "traceparent");
                String[] strs = msg.split(",");
                BatchResponse batchResponse = new BatchResponse(datum.getId());
                UserMetadata userMetadata = datum.getUserMetadata().getGroups().isEmpty()
                        ? null
                        : new UserMetadata(datum.getUserMetadata());
                for (String str : strs) {
                    batchResponse.append(new Message(
                            str.getBytes(),
                            null,
                            null,
                            userMetadata));
                }
                batchResponses.append(batchResponse);
            }
            return batchResponses;
        }
    }
}
