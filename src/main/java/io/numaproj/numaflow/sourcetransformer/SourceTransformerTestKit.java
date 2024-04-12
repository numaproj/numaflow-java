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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
     * @param sourceTransformer the source transformer to test
     */
    public SourceTransformerTestKit(SourceTransformer sourceTransformer) {
        this(
                sourceTransformer,
                GRPCConfig
                        .newBuilder()
                        .isLocal(true)
                        .build());
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
     * SourceTransformerClient is a client for sending requests to the source transformer server.
     */
    public static class Client {
        private final ManagedChannel channel;
        private final SourceTransformGrpc.SourceTransformStub sourceTransformStub;

        /**
         * empty constructor for Client.
         * default host is localhost and port is 50051.
         */
        public Client() {
            this("localhost", Constants.DEFAULT_PORT);
        }

        /**
         * constructor for Client with host and port.
         *
         * @param host the host to connect to
         * @param port the port to connect to
         */
        public Client(String host, int port) {
            this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
            this.sourceTransformStub = SourceTransformGrpc.newStub(channel);
        }

        /**
         * Send a gRPC request to the server.
         *
         * @param request the request to send
         *
         * @return a CompletableFuture that will be completed when the response is received
         */
        private CompletableFuture<Sourcetransformer.SourceTransformResponse> sendGrpcRequest(
                Sourcetransformer.SourceTransformRequest request) {
            CompletableFuture<Sourcetransformer.SourceTransformResponse> future = new CompletableFuture<>();
            StreamObserver<Sourcetransformer.SourceTransformResponse> responseObserver = new StreamObserver<>() {
                @Override
                public void onNext(Sourcetransformer.SourceTransformResponse response) {
                    future.complete(response);
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                    if (!future.isDone()) {
                        future.completeExceptionally(new RuntimeException(
                                "Server completed without a response"));
                    }
                }
            };

            sourceTransformStub.sourceTransformFn(
                    request, responseObserver);

            return future;
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
            Sourcetransformer.SourceTransformRequest request = Sourcetransformer.SourceTransformRequest
                    .newBuilder()
                    .addAllKeys(keys == null ? new ArrayList<>() : List.of(keys))
                    .setValue(data.getValue()
                            == null ? ByteString.EMPTY : ByteString.copyFrom(data.getValue()))
                    .setEventTime(
                            data.getEventTime() == null ? Timestamp.newBuilder().build() : Timestamp
                                    .newBuilder()
                                    .setSeconds(data.getEventTime().getEpochSecond())
                                    .setNanos(data.getEventTime().getNano())
                                    .build())
                    .setWatermark(
                            data.getWatermark() == null ? Timestamp.newBuilder().build() : Timestamp
                                    .newBuilder()
                                    .setSeconds(data.getWatermark().getEpochSecond())
                                    .setNanos(data.getWatermark().getNano())
                                    .build())
                    .build();

            try {
                Sourcetransformer.SourceTransformResponse response = this
                        .sendGrpcRequest(request)
                        .get();
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
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
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
