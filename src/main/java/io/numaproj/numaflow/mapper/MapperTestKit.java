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
        private final StreamObserver<MapOuterClass.MapRequest> requestStreamObserver;
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
        private final ConcurrentHashMap<String, CompletableFuture<MapOuterClass.MapResponse>> responseFutures = new ConcurrentHashMap<>();

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
            MapGrpc.MapStub stub = MapGrpc.newStub(channel);
            this.requestStreamObserver = stub.mapFn(new ResponseObserver());
            this.requestStreamObserver.onNext(MapOuterClass.MapRequest.newBuilder()
                    .setHandshake(MapOuterClass.Handshake.newBuilder().setSot(true))
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
            String requestId = UUID
                    .randomUUID()
                    .toString();
            CompletableFuture<MapOuterClass.MapResponse> responseFuture = new CompletableFuture<>();
            responseFutures.put(requestId, responseFuture);

            MapOuterClass.MapRequest request = createRequest(keys, data, requestId);
            try {
                this.requestStreamObserver.onNext(request);
                MapOuterClass.MapResponse response = responseFuture.get();
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

        private MapOuterClass.MapRequest createRequest(
                String[] keys,
                Datum data,
                String requestId) {
            return MapOuterClass.MapRequest.newBuilder().setRequest(
                    MapOuterClass.MapRequest.Request.newBuilder()
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
                            .build()
            ).setId(requestId).build();
        }

        private MessageList createResponse(MapOuterClass.MapResponse response) {
            List<Message> messages = response.getResultsList().stream()
                    .map(result -> new Message(
                            result.getValue().toByteArray(),
                            result.getKeysList().toArray(new String[0]),
                            result.getTagsList().toArray(new String[0])))
                    .collect(Collectors.toList());
            return new MessageList(messages);
        }

        private class ResponseObserver implements StreamObserver<MapOuterClass.MapResponse> {
            @Override
            public void onNext(MapOuterClass.MapResponse mapResponse) {
                if (mapResponse.hasHandshake()) {
                    return;
                }
                CompletableFuture<MapOuterClass.MapResponse> responseFuture = responseFutures.get(
                        mapResponse.getId());
                if (responseFuture != null) {
                    responseFuture.complete(mapResponse);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                // complete all remaining futures exceptionally
                for (CompletableFuture<MapOuterClass.MapResponse> future : responseFutures.values()) {
                    future.completeExceptionally(throwable);
                }
            }

            @Override
            public void onCompleted() {
                // remove all completed futures
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
