package io.numaproj.numaflow.mapper;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.map.v1.MapGrpc;
import io.numaproj.numaflow.map.v1.MapOuterClass;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * MapperTestKit is a test kit for testing Mapper implementations.
 * It provides methods to start and stop the server and send requests to the server.
 */
@Slf4j
public class MapperTestKit {
    private final Mapper mapper;
    private final GRPCConfig grpcConfig;
    private Server server;

    /**
     * Create a new MapperTestKit with the given Mapper.
     *
     * @param mapper the mapper to test
     */
    public MapperTestKit(Mapper mapper) {
        this(mapper, GRPCConfig.defaultGrpcConfig());
    }

    /**
     * Create a new MapperTestKit with the given Mapper and GRPCConfig.
     *
     * @param mapper the mapper to test
     * @param grpcConfig the grpc configuration to use.
     */
    public MapperTestKit(Mapper mapper, GRPCConfig grpcConfig) {
        this.mapper = mapper;
        this.grpcConfig = grpcConfig;
    }

    /**
     * Start the server.
     *
     * @throws Exception if server fails to start
     */
    public void startServer() throws Exception {
        server = new Server(this.mapper, this.grpcConfig);
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
     * Client is a client for sending requests to the map server.
     */
    public static class Client {
        private final ManagedChannel channel;
        private final MapGrpc.MapStub mapStub;

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
            this.mapStub = MapGrpc.newStub(channel);
        }

        private CompletableFuture<MapOuterClass.MapResponse> sendGrpcRequest(MapOuterClass.MapRequest request) {
            CompletableFuture<MapOuterClass.MapResponse> future = new CompletableFuture<>();
            StreamObserver<MapOuterClass.MapResponse> responseObserver = new StreamObserver<>() {
                @Override
                public void onNext(MapOuterClass.MapResponse response) {
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

            mapStub.mapFn(
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
            MapOuterClass.MapRequest request = MapOuterClass.MapRequest.newBuilder()
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
                    .putAllHeaders(data.getHeaders() == null ? new HashMap<>() : data.getHeaders())
                    .build();

            try {
                MapOuterClass.MapResponse response = this.sendGrpcRequest(request).get();
                List<Message> messages = response.getResultsList().stream()
                        .map(result -> new Message(
                                result.getValue().toByteArray(),
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
