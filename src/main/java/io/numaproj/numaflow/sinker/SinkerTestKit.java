package io.numaproj.numaflow.sinker;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.shared.SystemMetadata;
import io.numaproj.numaflow.shared.UserMetadata;
import io.numaproj.numaflow.sink.v1.SinkGrpc;
import io.numaproj.numaflow.sink.v1.SinkOuterClass;
import jdk.jfr.Experimental;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * SinkerTestKit is a test kit for testing Sinker implementations.
 * It provides methods to start and stop the server and send requests to the server.
 * It also provides a simple implementation of Datum for testing.
 * It also provides a simple client to send requests to the server.
 */
@Experimental
@Slf4j
public class SinkerTestKit {

    private final Sinker sinker;
    private final GRPCConfig grpcConfig;
    private Server server;

    /**
     * Create a new SinkerTestKit.
     *
     * @param sinker the sinker to test
     */
    public SinkerTestKit(Sinker sinker) {
        this(sinker, GRPCConfig.defaultGrpcConfig());
    }

    /**
     * Create a new SinkerTestKit with the given Sinker and GRPCConfig.
     *
     * @param sinker the sinker to test
     * @param grpcConfig the grpc configuration to use
     */
    public SinkerTestKit(Sinker sinker, GRPCConfig grpcConfig) {
        this.sinker = sinker;
        this.grpcConfig = grpcConfig;
    }

    /**
     * Start the server.
     *
     * @throws IOException if server fails to start
     */
    public void startServer() throws Exception {
        server = new Server(sinker, grpcConfig);
        server.start();
    }

    /**
     * Stop the server.
     *
     * @throws InterruptedException if server fails to stop
     */
    public void stopServer() throws InterruptedException {
        if (server != null) {
            server.stop();
        }
    }

    /**
     * Client is a client for sending requests to the server.
     */
    public static class Client {
        private final ManagedChannel channel;
        private final SinkGrpc.SinkStub sinkStub;

        /**
         * Create a new Client with default host and port.
         * Default host is localhost and default port is 50051.
         */
        public Client() {
            this(Constants.DEFAULT_HOST, Constants.DEFAULT_PORT);
        }

        /**
         * Create a new Client with the given host and port.
         *
         * @param host the host to connect to
         * @param port the port to connect to
         */
        public Client(String host, int port) {
            this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
            this.sinkStub = SinkGrpc.newStub(channel);
        }


        /**
         * Send request to the server.
         *
         * @param datumIterator iterator of Datum objects to send to the server
         *
         * @return response from the server as a ResponseList
         */
        public ResponseList sendRequest(DatumIterator datumIterator) {
            List<SinkOuterClass.SinkResponse> responses = new ArrayList<>();
            CompletableFuture<List<SinkOuterClass.SinkResponse>> future = new CompletableFuture<>();

            StreamObserver<SinkOuterClass.SinkResponse> responseObserver = new StreamObserver<>() {
                @Override
                public void onNext(SinkOuterClass.SinkResponse response) {
                    responses.add(response);
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                    future.complete(responses);
                }
            };

            StreamObserver<SinkOuterClass.SinkRequest> requestObserver = sinkStub.sinkFn(
                    responseObserver);

            // send handshake request
            requestObserver.onNext(SinkOuterClass.SinkRequest.newBuilder()
                    .setHandshake(SinkOuterClass.Handshake.newBuilder().setSot(true).build())
                    .build());

            // send actual requests
            while (true) {
                Datum datum = null;
                try {
                    datum = datumIterator.next();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    continue;
                }
                if (datum == null) {
                    break;
                }
                SinkOuterClass.SinkRequest.Request request = SinkOuterClass.SinkRequest.Request
                        .newBuilder()
                        .addAllKeys(
                                datum.getKeys()
                                        == null ? new ArrayList<>() : Arrays.asList(datum.getKeys()))
                        .setValue(datum.getValue() == null ? ByteString.EMPTY : ByteString.copyFrom(
                                datum.getValue()))
                        .setId(datum.getId())
                        .setEventTime(datum.getEventTime() == null ? Timestamp
                                .newBuilder()
                                .build() : Timestamp.newBuilder()
                                .setSeconds(datum.getEventTime().getEpochSecond())
                                .setNanos(datum.getEventTime().getNano()).build())
                        .setWatermark(datum.getWatermark() == null ? Timestamp
                                .newBuilder()
                                .build() : Timestamp.newBuilder()
                                .setSeconds(datum.getWatermark().getEpochSecond())
                                .setNanos(datum.getWatermark().getNano()).build())
                        .putAllHeaders(
                                datum.getHeaders() == null ? new HashMap<>() : datum.getHeaders())
                        .build();
                requestObserver.onNext(SinkOuterClass.SinkRequest
                        .newBuilder()
                        .setRequest(request)
                        .build());
            }
            // send end of transmission message
            requestObserver.onNext(SinkOuterClass.SinkRequest.newBuilder().setStatus(
                    SinkOuterClass.TransmissionStatus.newBuilder().setEot(true)).build());

            requestObserver.onCompleted();

            List<SinkOuterClass.SinkResponse> outputResponses;
            try {
                outputResponses = future.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            ResponseList.ResponseListBuilder responseListBuilder = ResponseList.newBuilder();
            for (SinkOuterClass.SinkResponse result : outputResponses) {
                if (result.getHandshake().getSot()) {
                    continue;
                }

                if (result.hasStatus() && result.getStatus().getEot()) {
                    continue;
                }

                for (SinkOuterClass.SinkResponse.Result response : result.getResultsList()) {
                    if (response.getStatus() == SinkOuterClass.Status.SUCCESS) {
                        responseListBuilder.addResponse(Response.responseOK(response
                                .getId()));
                    } else if (response.getStatus() == SinkOuterClass.Status.FALLBACK) {
                        responseListBuilder.addResponse(Response.responseFallback(
                                response.getId()));
                    } else {
                        responseListBuilder.addResponse(Response.responseFailure(
                                response.getId(), response.getErrMsg()));
                    }
                }
            }

            return responseListBuilder.build();
        }

        /**
         * close the client.
         *
         * @throws InterruptedException if client fails to close
         */
        public void close() throws InterruptedException {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * TestListIterator is a list based DatumIterator for testing.
     */
    @Getter
    @Setter
    public static class TestListIterator implements DatumIterator {
        private final List<Datum> data;
        private int index;

        public TestListIterator() {
            this.data = new ArrayList<>();
            this.index = 0;
        }

        @Override
        public Datum next() throws InterruptedException {
            if (index < data.size()) {
                return data.get(index++);
            }
            return null;
        }

        public void addDatum(Datum datum) {
            data.add(datum);
        }
    }

    /**
     * TestBlockingIterator is a blocking queue based DatumIterator for testing.
     * It has a queue size of 1. Users can use this to stream data to the server.
     * If the queue is full, the iterator will block until the queue has space.
     * users should invoke close() to indicate the end of the stream to the server.
     */
    public static class TestBlockingIterator implements DatumIterator {
        private final LinkedBlockingQueue<Datum> queue;
        private volatile boolean closed = false;

        public TestBlockingIterator() {
            this.queue = new LinkedBlockingQueue<>(1); // set the queue size to 10
        }

        @Override
        public Datum next() throws InterruptedException {
            if (!closed) {
                return queue.take();
            }
            return null;
        }

        public void addDatum(Datum datum) throws InterruptedException {
            if (!closed) {
                queue.put(datum);
            }
        }

        public void close() {
            closed = true;
        }
    }

    /**
     * TestDatum is a Datum for testing.
     */
    @Getter
    @Builder
    public static class TestDatum implements Datum {
        private final String id;
        private final byte[] value;
        private final String[] keys;
        private final Instant eventTime;
        private final Instant watermark;
        private final Map<String, String> headers;
        private final UserMetadata userMetadata;
        private final SystemMetadata systemMetadata;

    }
}
