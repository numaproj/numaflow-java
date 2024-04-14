package io.numaproj.numaflow.reducer;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.reduce.v1.ReduceGrpc;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.numaproj.numaflow.shared.GrpcServerUtils.WIN_END_KEY;
import static io.numaproj.numaflow.shared.GrpcServerUtils.WIN_START_KEY;

/**
 * ReducerTestKit is a test kit for testing Reducer implementations.
 * It provides methods to start and stop the server and send requests to the server.
 * It also provides a simple implementation of Datum for testing.
 * It also provides a simple client to send requests to the server.
 */
public class ReducerTestKit {
    private final ReducerFactory<? extends Reducer> reducer;
    private final GRPCConfig grpcConfig;
    private Server server;

    public ReducerTestKit(ReducerFactory<? extends Reducer> reducer) {
        this(reducer, GRPCConfig.defaultGrpcConfig());
    }

    public ReducerTestKit(ReducerFactory<? extends Reducer> reducer, GRPCConfig grpcConfig) {
        this.reducer = reducer;
        this.grpcConfig = grpcConfig;
    }

    /**
     * startServer starts the server.
     *
     * @throws Exception if server fails to start
     */
    public void startServer() throws Exception {
        server = new Server(reducer, grpcConfig);
        server.start();
    }

    /**
     * stopServer stops the server.
     *
     * @throws InterruptedException if server fails to stop
     */
    public void stopServer() throws InterruptedException {
        if (server != null) {
            server.stop();
        }
    }

    /**
     * Client is a client to send requests to the server.
     * It provides a method to send a reduce request to the server.
     */
    public static class Client {
        private final ManagedChannel channel;
        private final ReduceGrpc.ReduceStub reduceStub;

        /**
         * Create a new Client with the default host and port.
         * The default host is localhost and the default port is 50051.
         */
        public Client() {
            this(Constants.DEFAULT_HOST, Constants.DEFAULT_PORT);
        }

        /**
         * Create a new Client with the given host and port.
         *
         * @param host the host
         * @param port the port
         */
        public Client(String host, int port) {
            this.channel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();
            this.reduceStub = ReduceGrpc.newStub(channel);
        }

        /**
         * sendReduceRequest sends a reduce request to the server.
         *
         * @param testReduceRequest the request to send
         *
         * @return the response from the server
         *
         * @throws Exception if the request fails
         */
        public MessageList sendReduceRequest(TestReduceRequest testReduceRequest) throws Exception {
            List<ReduceOuterClass.ReduceRequest.Payload> payloadList = new ArrayList<>();
            Metadata metadata = new io.grpc.Metadata();
            metadata.put(
                    Metadata.Key.of(WIN_START_KEY, Metadata.ASCII_STRING_MARSHALLER),
                    String.valueOf(testReduceRequest.getStartTime().toEpochMilli()));
            metadata.put(
                    Metadata.Key.of(WIN_END_KEY, Metadata.ASCII_STRING_MARSHALLER),
                    String.valueOf(testReduceRequest.getEndTime().toEpochMilli()));

            for (Datum datum : testReduceRequest.getDatumList()) {
                ReduceOuterClass.ReduceRequest.Payload.Builder payloadBuilder =
                        ReduceOuterClass.ReduceRequest.Payload.newBuilder()
                                .setValue(datum.getValue()
                                        != null ? ByteString.copyFrom(datum.getValue()) : ByteString.EMPTY)
                                .setEventTime(datum.getEventTime() == null ? Timestamp
                                        .newBuilder()
                                        .build() : Timestamp
                                        .newBuilder()
                                        .setSeconds(datum.getEventTime().getEpochSecond())
                                        .setNanos(datum.getEventTime().getNano())
                                        .build())
                                .setWatermark(datum.getWatermark() == null ? Timestamp
                                        .newBuilder()
                                        .build() : Timestamp
                                        .newBuilder()
                                        .setSeconds(datum.getWatermark().getEpochSecond())
                                        .setNanos(datum.getWatermark().getNano())
                                        .build())
                                .addAllKeys(Arrays.asList(testReduceRequest.getKeys()))
                                .putAllHeaders(datum.getHeaders()
                                        == null ? new HashMap<>() : datum.getHeaders());
                payloadList.add(payloadBuilder.build());
            }

            List<ReduceOuterClass.ReduceResponse> responseList = new ArrayList<>();
            CompletableFuture<Boolean> responseFuture = new CompletableFuture<>();
            StreamObserver<ReduceOuterClass.ReduceRequest> requestObserver = reduceStub
                    .withInterceptors(
                            MetadataUtils.newAttachHeadersInterceptor(metadata))
                    .reduceFn(new StreamObserver<>() {
                        @Override
                        public void onNext(ReduceOuterClass.ReduceResponse value) {
                            responseList.add(value);
                        }

                        @Override
                        public void onError(Throwable t) {
                            responseFuture.completeExceptionally(t);
                        }

                        @Override
                        public void onCompleted() {
                            responseFuture.complete(true);
                        }
                    });

            for (ReduceOuterClass.ReduceRequest.Payload payload : payloadList) {
                // create a window from the start and end time
                ReduceOuterClass.Window window = ReduceOuterClass.Window.newBuilder()
                        .setStart(Timestamp
                                .newBuilder()
                                .setSeconds(testReduceRequest.getStartTime().getEpochSecond())
                                .setNanos(testReduceRequest.getStartTime().getNano())
                                .build())
                        .setEnd(Timestamp
                                .newBuilder()
                                .setSeconds(testReduceRequest.getEndTime().getEpochSecond())
                                .setNanos(testReduceRequest.getEndTime().getNano())
                                .build())
                        .build();

                // create a request with the payload and window
                ReduceOuterClass.ReduceRequest grpcRequest = ReduceOuterClass.ReduceRequest
                        .newBuilder()
                        .setPayload(payload)
                        .setOperation(ReduceOuterClass.ReduceRequest.WindowOperation
                                .newBuilder()
                                .setEvent(ReduceOuterClass.ReduceRequest.WindowOperation.Event.APPEND)
                                .addWindows(window)
                                .build())
                        .build();
                requestObserver.onNext(grpcRequest);
            }
            requestObserver.onCompleted();
            responseFuture.get();

            MessageList.MessageListBuilder messageListBuilder = MessageList.newBuilder();
            for (ReduceOuterClass.ReduceResponse response : responseList) {
                if (response.getEOF()) {
                    break;
                }
                messageListBuilder.addMessage(new Message(
                        response.getResult().getValue().toByteArray(),
                        response.getResult().getKeysList().toArray(new String[0]),
                        response.getResult().getTagsList().toArray(new String[0])
                ));
            }
            return messageListBuilder.build();
        }

        /**
         * close the client.
         */
        public void close() {
            channel.shutdown();
        }
    }

    /**
     * TestReduceRequest is the request to send to the server for testing.
     * It contains a list of Datum, keys, start time and end time.
     */
    @Getter
    @Builder
    @Setter
    public static class TestReduceRequest {
        private List<Datum> datumList;
        private String[] keys;
        private Instant startTime;
        private Instant endTime;
    }

    /**
     * TestDatum is a simple implementation of Datum for testing.
     */
    @Builder
    @Getter
    public static class TestDatum implements Datum {
        private byte[] value;
        private Instant eventTime;
        private Instant watermark;
        private Map<String, String> headers;
    }

}
