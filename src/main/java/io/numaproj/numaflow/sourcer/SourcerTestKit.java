package io.numaproj.numaflow.sourcer;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.source.v1.SourceGrpc;
import io.numaproj.numaflow.source.v1.SourceOuterClass;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * SourcerTestKit is a test kit for testing Sourcer implementations.
 * It provides methods to start and stop the server and send requests to the server.
 * It also provides simple implementations of ReadRequest, AckRequest and OutputObserver for testing.
 * It also provides a simple client to send requests to the server.
 */
public class SourcerTestKit {
    private final Sourcer sourcer;
    private final GRPCConfig grpcConfig;
    private Server server;

    /**
     * Create a new SourcerTestKit with the given Sourcer.
     *
     * @param sourcer the sourcer to test
     */
    public SourcerTestKit(Sourcer sourcer) {
        this(sourcer, GRPCConfig.defaultGrpcConfig());
    }

    /**
     * Create a new SourcerTestKit with the given Sourcer and GRPCConfig.
     *
     * @param sourcer the sourcer to test
     * @param grpcConfig the grpc configuration to use
     */
    public SourcerTestKit(Sourcer sourcer, GRPCConfig grpcConfig) {
        this.sourcer = sourcer;
        this.grpcConfig = grpcConfig;
    }

    /**
     * startServer starts the server.
     *
     * @throws Exception if server fails to start
     */
    public void startServer() throws Exception {
        server = new Server(sourcer, grpcConfig);
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
     * SourcerClient is a client to send requests to the server.
     * It provides methods to send read, ack and pending requests to the server.
     */
    public static class SourcerClient {
        private final ManagedChannel channel;
        private final SourceGrpc.SourceStub sourceStub;

        /**
         * Create a new SourcerClient with the default host and port.
         * The default host is localhost and the default port is 50051.
         */
        public SourcerClient() {
            this("localhost", 50051);
        }

        /**
         * Create a new SourcerClient with the given host and port.
         *
         * @param host the host
         * @param port the port
         */
        public SourcerClient(String host, int port) {
            this.channel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();
            this.sourceStub = SourceGrpc.newStub(channel);
        }

        /**
         * close closes the client.
         *
         * @throws InterruptedException if the client fails to close
         */
        public void close() throws InterruptedException {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }

        /**
         * sendReadRequest sends a read request to the server.
         *
         * @param request the read request
         * @param observer the output observer to receive the messages
         *
         * @throws Exception if the request fails
         */
        public void sendReadRequest(ReadRequest request, OutputObserver observer) throws Exception {
            SourceOuterClass.ReadRequest grpcRequest = SourceOuterClass.ReadRequest.newBuilder()
                    .setRequest(SourceOuterClass.ReadRequest.Request.newBuilder()
                            .setNumRecords(request.getCount())
                            .setTimeoutInMs((int) request.getTimeout().toMillis())
                            .build())
                    .build();

            CompletableFuture<Boolean> future = new CompletableFuture<>();
            sourceStub.readFn(grpcRequest, new StreamObserver<>() {
                @Override
                public void onNext(SourceOuterClass.ReadResponse value) {
                    Message message = new Message(
                            value.getResult().getPayload().toByteArray(),
                            new Offset(
                                    value.getResult().getOffset().getOffset().toByteArray(),
                                    value.getResult().getOffset().getPartitionId()),
                            Instant.ofEpochSecond(
                                    value.getResult().getEventTime().getSeconds(),
                                    value.getResult().getEventTime().getNanos()
                            ),
                            value.getResult().getKeysList().toArray(new String[0]),
                            value.getResult().getHeadersMap());
                    observer.send(message);
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                    future.complete(true);
                }
            });
            future.get();
        }

        /**
         * sendAckRequest sends an ack request to the server.
         *
         * @param request the ack request
         *
         * @throws Exception if the request fails
         */
        public void sendAckRequest(AckRequest request) throws Exception {
            CompletableFuture<SourceOuterClass.AckResponse> future = new CompletableFuture<>();
            SourceOuterClass.AckRequest.Request.Builder builder = SourceOuterClass.AckRequest.Request.newBuilder();
            for (Offset offset : request.getOffsets()) {
                builder.addOffsets(SourceOuterClass.Offset.newBuilder()
                        .setOffset(com.google.protobuf.ByteString.copyFrom(offset.getValue()))
                        .setPartitionId(offset.getPartitionId())
                        .build());
            }

            SourceOuterClass.AckRequest grpcRequest = SourceOuterClass.AckRequest.newBuilder()
                    .setRequest(builder.build())
                    .build();

            sourceStub.ackFn(grpcRequest, new StreamObserver<>() {
                @Override
                public void onNext(SourceOuterClass.AckResponse value) {
                    future.complete(value);
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
            });
            future.get();
        }

        /**
         * sendPendingRequest sends a pending request to the server.
         *
         * @return the number of pending messages
         *
         * @throws Exception if the request fails
         */
        public long sendPendingRequest() throws Exception {
            CompletableFuture<SourceOuterClass.PendingResponse> future = new CompletableFuture<>();
            StreamObserver<SourceOuterClass.PendingResponse> observer = new StreamObserver<>() {

                @Override
                public void onNext(SourceOuterClass.PendingResponse value) {
                    future.complete(value);
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
            sourceStub.pendingFn(Empty.newBuilder().build(), observer);
            return future.get().getResult().getCount();
        }
    }


    /**
     * TestReadRequest is a simple implementation of ReadRequest for testing.
     */
    @Getter
    @Setter
    @Builder
    public static class TestReadRequest implements ReadRequest {
        private long count;
        private Duration timeout;
    }

    /**
     * TestAckRequest is a simple implementation of AckRequest for testing.
     */
    @Getter
    @Setter
    @Builder
    public static class TestAckRequest implements AckRequest {
        List<Offset> offsets;
    }

    /**
     * TestListBasedObserver is a simple list based implementation of OutputObserver for testing.
     */
    @Getter
    @Setter
    public static class TestListBasedObserver implements OutputObserver {
        private List<Message> messages = new ArrayList<>();


        @Override
        public void send(Message message) {
            messages.add(message);
        }

    }

}
