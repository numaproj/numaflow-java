package io.numaproj.numaflow.sourcetransformer;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sourcetransformer.v1.SourceTransformGrpc;
import io.numaproj.numaflow.sourcetransformer.v1.Sourcetransformer;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * SourceTransformerTestKit is a test kit for testing SourceTransformer implementations.
 * It provides methods to start and stop the server and send requests to the server.
 */
@Slf4j
public class SourceTransformerTestKit {
    private final SourceTransformer sourceTransformer;
    private final GRPCConfig grpcConfig;
    private Server server;

    /**
     * Create a new SourceTransformerTestKit with the given SourceTransformer.
     *
     * @param sourceTransformer the sourceTransformer to test
     */
    public SourceTransformerTestKit(SourceTransformer sourceTransformer) {
        this(sourceTransformer, GRPCConfig.defaultGrpcConfig());
    }

    /**
     * Create a new SourceTransformerTestKit with the given SourceTransformer and GRPCConfig.
     *
     * @param sourceTransformer the sourceTransformer to test
     * @param grpcConfig the grpc configuration to use.
     */
    public SourceTransformerTestKit(SourceTransformer sourceTransformer, GRPCConfig grpcConfig) {
        this.sourceTransformer = sourceTransformer;
        this.grpcConfig = grpcConfig;
    }

    /**
     * Start the server.
     *
     * @throws Exception if server fails to start
     */
    public void startServer() throws Exception {
        server = new Server(this.sourceTransformer, this.grpcConfig);
        server.start();
    }

    /**
     * Stops the server.
     *
     * @throws Exception if server fails to stop
     */
    public void stopServer() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    /**
     * Client is a client for sending requests to the sourceTransform server.
     */
    public static class Client {
        private final ManagedChannel channel;
        private final StreamObserver<Sourcetransformer.SourceTransformRequest> requestStreamObserver;
        /*
         * A ConcurrentHashMap that stores CompletableFuture instances for each request sent to the server.
         * Each CompletableFuture corresponds to a unique request and is used to handle the response from the server.
         *
         * Key: A unique identifier (UUID) for each request sent to the server.
         * Value: A CompletableFuture that will be completed when the server sends a response for the corresponding request.
         *
         * We use a concurrent map so that the user can send multiple requests concurrently.
         *
         * When a request is sent to the server, a new CompletableFuture is created and stored in the map with its unique identifier.
         * When a response is received from the server, the corresponding CompletableFuture is retrieved from the map using the unique identifier,
         * and then completed with the response data. If an error occurs, we complete all remaining futures exceptionally.
         */
        private final ConcurrentHashMap<String, CompletableFuture<Sourcetransformer.SourceTransformResponse>> responseFutures = new ConcurrentHashMap<>();

        /**
         * empty constructor for Client.
         * default host is localhost and port is 50051.
         */
        public Client() {
            this(Constants.DEFAULT_HOST, Constants.DEFAULT_PORT);
        }

        /**
         * constructor for Client with host and port.
         *
         * @param host the host to connect to
         * @param port the port to connect to
         */
        public Client(String host, int port) {
            this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
            SourceTransformGrpc.SourceTransformStub stub = SourceTransformGrpc.newStub(channel);
            this.requestStreamObserver = stub.sourceTransformFn(new ResponseObserver());
            this.requestStreamObserver.onNext(Sourcetransformer.SourceTransformRequest.newBuilder()
                    .setHandshake(Sourcetransformer.Handshake.newBuilder().setSot(true))
                    .build());
        }

        /**
         * Send a request to the server.
         *
         * @param keys keys to send in the request
         * @param data data to send in the request
         *
         * @return response from the server as a MessageList
         */
        public MessageList sendRequest(String[] keys, Datum data) {
            String requestId = UUID.randomUUID().toString();
            CompletableFuture<Sourcetransformer.SourceTransformResponse> responseFuture = new CompletableFuture<>();
            responseFutures.put(requestId, responseFuture);

            Sourcetransformer.SourceTransformRequest request = createRequest(keys, data, requestId);
            try {
                this.requestStreamObserver.onNext(request);
                Sourcetransformer.SourceTransformResponse response = responseFuture.get();
                MessageList messageList = createResponse(response);
                responseFutures.remove(requestId);
                return messageList;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Close the client.
         *
         * @throws InterruptedException if the client fails to close
         */
        public void close() throws InterruptedException {
            this.requestStreamObserver.onCompleted();
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }

        private Sourcetransformer.SourceTransformRequest createRequest(
                String[] keys,
                Datum data,
                String requestId) {
            return Sourcetransformer.SourceTransformRequest.newBuilder().setRequest(
                    Sourcetransformer.SourceTransformRequest.Request.newBuilder()
                            .addAllKeys(keys == null ? new ArrayList<>() : Arrays.asList(keys))
                            .setValue(data.getValue()
                                    == null ? ByteString.EMPTY : ByteString.copyFrom(data.getValue()))
                            .setEventTime(
                                    data.getEventTime() == null ? Timestamp
                                            .newBuilder()
                                            .build() : Timestamp.newBuilder()
                                            .setSeconds(data.getEventTime().getEpochSecond())
                                            .setNanos(data.getEventTime().getNano())
                                            .build())
                            .setWatermark(
                                    data.getWatermark() == null ? Timestamp
                                            .newBuilder()
                                            .build() : Timestamp.newBuilder()
                                            .setSeconds(data.getWatermark().getEpochSecond())
                                            .setNanos(data.getWatermark().getNano())
                                            .build())
                            .putAllHeaders(
                                    data.getHeaders() == null ? new HashMap<>() : data.getHeaders())
                            .setId(requestId)
                            .build()
            ).build();
        }

        private MessageList createResponse(Sourcetransformer.SourceTransformResponse response) {
            List<Message> messages = response.getResultsList().stream()
                    .map(result -> new Message(
                            result.getValue().toByteArray(),
                            Instant.ofEpochSecond(
                                    result.getEventTime().getSeconds(),
                                    result.getEventTime().getNanos()),
                            result.getKeysList().toArray(new String[0]),
                            result.getTagsList().toArray(new String[0])))
                    .collect(Collectors.toList());
            return new MessageList(messages);
        }

        private class ResponseObserver implements StreamObserver<Sourcetransformer.SourceTransformResponse> {
            @Override
            public void onNext(Sourcetransformer.SourceTransformResponse sourceTransformResponse) {
                if (sourceTransformResponse.hasHandshake()) {
                    return;
                }
                CompletableFuture<Sourcetransformer.SourceTransformResponse> responseFuture = responseFutures.get(
                        sourceTransformResponse.getId());
                if (responseFuture != null) {
                    responseFuture.complete(sourceTransformResponse);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                for (CompletableFuture<Sourcetransformer.SourceTransformResponse> future : responseFutures.values()) {
                    future.completeExceptionally(throwable);
                }
            }

            @Override
            public void onCompleted() {
                responseFutures.values().removeIf(CompletableFuture::isDone);
            }
        }
    }

    /**
     * TestDatum is a Datum for testing.
     */
    @Getter
    @Builder
    public static class TestDatum implements Datum {
        private final byte[] value;
        private final Instant eventTime;
        private final Instant watermark;
        private final Map<String, String> headers;
    }
}
