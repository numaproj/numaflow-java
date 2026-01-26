package io.numaproj.numaflow.sourcetransformer;

import com.google.protobuf.ByteString;
import common.MetadataOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.shared.UserMetadata;
import io.numaproj.numaflow.sourcetransformer.v1.SourceTransformGrpc;
import io.numaproj.numaflow.sourcetransformer.v1.Sourcetransformer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ServerTest {
    private final static String PROCESSED_KEY_SUFFIX = "-key-processed";
    private final static String PROCESSED_VALUE_SUFFIX = "-value-processed";
    private final static Instant TEST_EVENT_TIME = Instant.MIN;

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
                new TestSourceTransformer(),
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
    public void testSourceTransformerSuccess() {
        Sourcetransformer.SourceTransformRequest handshakeRequest = Sourcetransformer.SourceTransformRequest
                .newBuilder()
                .setHandshake(Sourcetransformer.Handshake
                        .newBuilder()
                        .setSot(true)
                        .build())
                .build();

        // Build user metadata and use it to initialize metadata to be added to the message passed to mapper
        Map<String, MetadataOuterClass.KeyValueGroup> prevVertexUserMetadata = new HashMap<>();
        prevVertexUserMetadata.put("prev-group",
                MetadataOuterClass.KeyValueGroup
                        .newBuilder()
                        .putKeyValue("prev-key", ByteString.copyFromUtf8("prev-value"))
                        .build()
        );
        MetadataOuterClass.Metadata metadata = MetadataOuterClass.Metadata
                .newBuilder()
                .putAllUserMetadata(prevVertexUserMetadata)
                .build();

        ByteString inValue = ByteString.copyFromUtf8("invalue");
        Sourcetransformer.SourceTransformRequest.Request inDatum = Sourcetransformer.SourceTransformRequest.Request
                .newBuilder()
                .setValue(inValue)
                .addAllKeys(List.of("test-st-key"))
                .setMetadata(metadata)
                .build();

        Sourcetransformer.SourceTransformRequest request = Sourcetransformer.SourceTransformRequest
                .newBuilder()
                .setRequest(inDatum)
                .build();

        String[] expectedKeys = new String[]{"test-st-key" + PROCESSED_KEY_SUFFIX};
        ByteString expectedValue = ByteString.copyFromUtf8("invalue" + PROCESSED_VALUE_SUFFIX);

        TransformerOutputStreamObserver responseObserver = new TransformerOutputStreamObserver(4);

        var stub = SourceTransformGrpc.newStub(inProcessChannel);
        var requestStreamObserver = stub
                .sourceTransformFn(responseObserver);

        requestStreamObserver.onNext(handshakeRequest);
        requestStreamObserver.onNext(request);
        requestStreamObserver.onNext(request);
        requestStreamObserver.onNext(request);

        try {
            responseObserver.done.get();
        } catch (InterruptedException | ExecutionException e) {
            fail("Error while waiting for response" + e.getMessage());
        }

        List<Sourcetransformer.SourceTransformResponse> responses = responseObserver.getResponses();
        assertEquals(4, responses.size());

        // first response is the handshake response
        assertEquals(handshakeRequest.getHandshake(), responses.get(0).getHandshake());

        responses = responses.subList(1, responses.size());
        for (Sourcetransformer.SourceTransformResponse response : responses) {
            assertEquals(expectedValue, response.getResults(0).getValue());
            assertEquals(Arrays.asList(expectedKeys), response.getResults(0).getKeysList());
            assertEquals(1, response.getResultsCount());
            // User metadata should be added to the response.
            // It should have both the previous metadata and the metadata added by the mapper.
            assertEquals(2, response.getResults(0).getMetadata().getUserMetadataMap().size());
            assertEquals("prev-value",
                    response.getResults(0)
                            .getMetadata()
                            .getUserMetadataMap()
                            .get("prev-group")
                            .getKeyValueMap()
                            .get("prev-key")
                            .toStringUtf8()
            );
            assertEquals("st-value",
                    response.getResults(0)
                            .getMetadata()
                            .getUserMetadataMap()
                            .get("st-group")
                            .getKeyValueMap()
                            .get("st-key")
                            .toStringUtf8()
            );
        }

        requestStreamObserver.onCompleted();
    }

    @Test
    public void testSourceTransformerWithoutHandshake() {
        ByteString inValue = ByteString.copyFromUtf8("invalue");
        Sourcetransformer.SourceTransformRequest.Request inDatum = Sourcetransformer.SourceTransformRequest.Request
                .newBuilder()
                .setValue(inValue)
                .addAllKeys(List.of("test-st-key"))
                .build();

        Sourcetransformer.SourceTransformRequest request = Sourcetransformer.SourceTransformRequest
                .newBuilder()
                .setRequest(inDatum)
                .build();

        TransformerOutputStreamObserver responseObserver = new TransformerOutputStreamObserver(1);

        var stub = SourceTransformGrpc.newStub(inProcessChannel);
        var requestStreamObserver = stub
                .sourceTransformFn(responseObserver);

        requestStreamObserver.onNext(request);

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

    private static class TestSourceTransformer extends SourceTransformer {
        @Override
        public MessageList processMessage(String[] keys, Datum datum) {
            String[] updatedKeys = Arrays
                    .stream(keys)
                    .map(c -> c + PROCESSED_KEY_SUFFIX)
                    .toArray(String[]::new);

            UserMetadata userMetadata = new UserMetadata(datum.getUserMetadata());
            userMetadata.addKV("st-group", "st-key", "st-value".getBytes());

            return MessageList
                    .newBuilder()
                    .addMessage(new Message(
                            (new String(datum.getValue())
                                    + PROCESSED_VALUE_SUFFIX).getBytes(),
                            TEST_EVENT_TIME,
                            updatedKeys,
                            new String[]{"test-tag"},
                            userMetadata))
                    .build();
        }
    }
}
