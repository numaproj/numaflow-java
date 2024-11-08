package io.numaproj.numaflow.mapper;

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
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ServerTest {
    private final static String PROCESSED_KEY_SUFFIX = "-key-processed";
    private final static String PROCESSED_VALUE_SUFFIX = "-value-processed";

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
                new TestMapFn(),
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
    public void testMapperSuccess() {
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

        MapOutputStreamObserver responseObserver = new MapOutputStreamObserver(4);

        var stub = MapGrpc.newStub(inProcessChannel);
        var requestStreamObserver = stub
                .mapFn(responseObserver);

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
        assertEquals(4, responses.size());

        // first response is the handshake response
        assertEquals(handshakeRequest.getHandshake(), responses.get(0).getHandshake());

        responses = responses.subList(1, responses.size());
        for (MapOuterClass.MapResponse response : responses) {
            assertEquals(expectedValue, response.getResults(0).getValue());
            assertEquals(Arrays.asList(expectedKeys), response.getResults(0).getKeysList());
            assertEquals(Arrays.asList(expectedTags), response.getResults(0).getTagsList());
            assertEquals(1, response.getResultsCount());
        }

        requestStreamObserver.onCompleted();
    }

    @Test
    public void testMapperWithoutHandshake() {
        ByteString inValue = ByteString.copyFromUtf8("invalue");
        MapOuterClass.MapRequest inDatum = MapOuterClass.MapRequest
                .newBuilder()
                .setRequest(MapOuterClass.MapRequest.Request
                        .newBuilder()
                        .setValue(inValue)
                        .addAllKeys(List.of("test-map-key"))
                        .build()).build();

        MapOutputStreamObserver responseObserver = new MapOutputStreamObserver(1);

        var stub = MapGrpc.newStub(inProcessChannel);
        var requestStreamObserver = stub
                .mapFn(responseObserver);

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

    private static class TestMapFn extends Mapper {
        @Override
        public MessageList processMessage(String[] keys, Datum datum) {
            String[] updatedKeys = Arrays
                    .stream(keys)
                    .map(c -> c + PROCESSED_KEY_SUFFIX)
                    .toArray(String[]::new);
            return MessageList
                    .newBuilder()
                    .addMessage(new Message(
                            (new String(datum.getValue())
                                    + PROCESSED_VALUE_SUFFIX).getBytes(),
                            updatedKeys,
                            new String[]{"test-tag"}))
                    .build();
        }
    }
}
