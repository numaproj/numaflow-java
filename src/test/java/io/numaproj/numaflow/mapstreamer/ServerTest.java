package io.numaproj.numaflow.mapstreamer;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.map.v1.MapGrpc;
import io.numaproj.numaflow.map.v1.MapOuterClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ServerTest {
    private static final String PROCESSED_KEY_SUFFIX = "-key-processed";
    private static final String PROCESSED_VALUE_SUFFIX = "-value-processed";

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private Service server;
    private ManagedChannel inProcessChannel;

    @Before
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();

        CompletableFuture<Void> shutdownSignal = new CompletableFuture<>();

        server = new Service(new TestMapStreamer(), shutdownSignal);

        grpcCleanup.register(InProcessServerBuilder
                .forName(serverName)
                .directExecutor()
                .addService(server)
                .build()
                .start());

        inProcessChannel = grpcCleanup.register(InProcessChannelBuilder
                .forName(serverName)
                .directExecutor()
                .build());
    }

    @After
    public void tearDown() {
        inProcessChannel.shutdownNow();
    }

    @Test
    public void testMapStreamerSuccess() {
        MapOuterClass.MapRequest handshakeRequest = MapOuterClass.MapRequest
                .newBuilder()
                .setHandshake(MapOuterClass.Handshake.newBuilder().setSot(true))
                .build();

        ByteString inValue = ByteString.copyFromUtf8("invalue");
        MapOuterClass.MapRequest inDatum = MapOuterClass.MapRequest
                .newBuilder()
                .setRequest(MapOuterClass.MapRequest.Request
                        .newBuilder()
                        .setValue(inValue)
                        .addAllKeys(List.of("test-map-key"))
                        .build()).build();

        String[] expectedKeys = new String[]{"test-map-key" + PROCESSED_KEY_SUFFIX};
        String[] expectedTags = new String[]{"test-tag"};
        ByteString expectedValue = ByteString.copyFromUtf8("invalue" + PROCESSED_VALUE_SUFFIX);

        MapStreamOutputStreamObserver responseObserver = new MapStreamOutputStreamObserver(7);

        var stub = MapGrpc.newStub(inProcessChannel);
        var requestStreamObserver = stub.mapFn(responseObserver);

        requestStreamObserver.onNext(handshakeRequest);
        requestStreamObserver.onNext(inDatum);
        requestStreamObserver.onNext(inDatum);
        requestStreamObserver.onNext(inDatum);

        try {
            responseObserver.done.get();
        } catch (InterruptedException | ExecutionException e) {
            fail("Error while waiting for response" + e.getMessage());
        }

        List<MapOuterClass.MapResponse> responses = responseObserver.getMapResponses();
        // we should have 7 responses, 1 handshake + 3 actual responses + 3 eofs
        assertEquals(7, responses.size());

        // first response is the handshake response
        assertEquals(handshakeRequest.getHandshake(), responses.get(0).getHandshake());

        responses = responses.subList(1, responses.size());
        for (MapOuterClass.MapResponse response : responses) {
            if (response.getStatus().getEot()) {
                continue;
            }
            assertEquals(expectedValue, response.getResults(0).getValue());
            assertEquals(Arrays.asList(expectedKeys), response.getResults(0).getKeysList());
            assertEquals(Arrays.asList(expectedTags), response.getResults(0).getTagsList());
            assertEquals(1, response.getResultsCount());
        }

        requestStreamObserver.onCompleted();
    }

    @Test
    public void testMapStreamerWithoutHandshake() {
        ByteString inValue = ByteString.copyFromUtf8("invalue");
        MapOuterClass.MapRequest inDatum = MapOuterClass.MapRequest
                .newBuilder()
                .setRequest(MapOuterClass.MapRequest.Request
                        .newBuilder()
                        .setValue(inValue)
                        .addAllKeys(List.of("test-map-key"))
                        .build()).build();

        MapStreamOutputStreamObserver responseObserver = new MapStreamOutputStreamObserver(1);

        var stub = MapGrpc.newStub(inProcessChannel);
        var requestStreamObserver = stub.mapFn(responseObserver);

        requestStreamObserver.onNext(inDatum);

        try {
            responseObserver.done.get();
            fail("Expected an exception to be thrown");
        } catch (InterruptedException | ExecutionException e) {
            assertEquals(
                    "io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Handshake request not received",
                    e.getMessage());
        }
        requestStreamObserver.onCompleted();
    }

    private static class TestMapStreamer extends MapStreamer {
        @Override
        public void processMessage(String[] keys, Datum datum, OutputObserver outputObserver) {
            String[] updatedKeys = Arrays.stream(keys)
                    .map(c -> c + PROCESSED_KEY_SUFFIX)
                    .toArray(String[]::new);

            Message message = new Message(
                    (new String(datum.getValue()) + PROCESSED_VALUE_SUFFIX).getBytes(),
                    updatedKeys,
                    new String[]{"test-tag"}
            );

            outputObserver.send(message);
        }
    }
}
