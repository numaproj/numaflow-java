package io.numaproj.numaflow.sinker;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sink.v1.SinkGrpc;
import io.numaproj.numaflow.sink.v1.SinkOuterClass;
import jdk.jfr.Experimental;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * SinkerTestKit is a test kit for testing Sinker implementations.
 * It provides methods to start and stop the server and send requests to the server.
 */
@Experimental
@Slf4j
public class SinkerTestKit {

    private static final int PORT = 50051;
    private final Service service;
    private Server server;
    private SinkClient client;

    /**
     * Create a new SinkerTestKit.
     *
     * @param sinker the sinker to test
     */
    public SinkerTestKit(Sinker sinker) {
        this.service = new Service(sinker);
    }

    /**
     * Start the server.
     *
     * @throws IOException if server fails to start
     */
    public void startServer() throws IOException {
        server = ServerBuilder.forPort(PORT)
                .addService(this.service)
                .build()
                .start();

        log.info("Server started, listening on {}", PORT);

        // Create a client for sending requests to the server
        client = new SinkClient("localhost", PORT);
    }

    /**
     * Stop the server.
     *
     * @throws InterruptedException if server fails to stop
     */
    public void stopServer() throws InterruptedException {
        // Shutdown the client and server
        if (client != null) {
            client.shutdown();
        }
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Send requests to the server.
     *
     * @param datumIterator iterator of Datum objects to send to the server
     *
     * @return response from the server as a ResponseList
     *
     * @throws InterruptedException if the thread is interrupted
     */
    public ResponseList sendRequests(DatumIterator datumIterator) throws Exception {
        ArrayList<SinkOuterClass.SinkRequest> requests = new ArrayList<>();
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
            SinkOuterClass.SinkRequest request = SinkOuterClass.SinkRequest.newBuilder()
                    .addAllKeys(
                            datum.getKeys() == null ? new ArrayList<>() : List.of(datum.getKeys()))
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
                    .build();
            requests.add(request);
        }

        SinkOuterClass.SinkResponse response;
        try {
            response = client.sendRequests(requests.iterator()).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


        ResponseList.ResponseListBuilder responseListBuilder = ResponseList.newBuilder();
        for (SinkOuterClass.SinkResponse.Result result : response.getResultsList()) {
            if (result.getSuccess()) {
                responseListBuilder.addResponse(Response.responseOK(result.getId()));
            } else {
                responseListBuilder.addResponse(Response.responseFailure(
                        result.getId(),
                        result.getErrMsg()));
            }
        }

        return responseListBuilder.build();
    }

    /**
     * SinkClient is a client for sending requests to the server.
     */
    private static class SinkClient {
        private final ManagedChannel channel;
        private final SinkGrpc.SinkStub sinkStub;

        public SinkClient(String host, int port) {
            this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
            this.sinkStub = SinkGrpc.newStub(channel);
        }

        public CompletableFuture<SinkOuterClass.SinkResponse> sendRequests(Iterator<SinkOuterClass.SinkRequest> requests) throws Exception {

            CompletableFuture<SinkOuterClass.SinkResponse> future = new CompletableFuture<>();
            StreamObserver<SinkOuterClass.SinkResponse> responseObserver = new StreamObserver<>() {
                @Override
                public void onNext(SinkOuterClass.SinkResponse response) {
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

            StreamObserver<SinkOuterClass.SinkRequest> requestObserver = sinkStub.sinkFn(
                    responseObserver);

            try {
                while (requests.hasNext()) {
                    SinkOuterClass.SinkRequest request = requests.next();
                    requestObserver.onNext(request);
                }
            } catch (RuntimeException e) {
                // Cancel rpc.
                requestObserver.onError(e);
                throw e;
            }

            requestObserver.onCompleted();
            return future;
        }

        public void shutdown() throws InterruptedException {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * TestDatumIterator is a DatumIterator for testing.
     */
    public static class TestDatumIterator implements DatumIterator {
        private final List<Datum> data;
        private int index;

        public TestDatumIterator() {
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
    }
}
