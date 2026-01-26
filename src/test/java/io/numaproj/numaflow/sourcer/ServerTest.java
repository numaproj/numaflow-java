package io.numaproj.numaflow.sourcer;

import com.google.protobuf.Empty;
import common.MetadataOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.shared.UserMetadata;
import io.numaproj.numaflow.source.v1.SourceGrpc;
import io.numaproj.numaflow.source.v1.SourceOuterClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ServerTest {

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
                new TestSourcer(),
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
    public void TestSourcer() {
        var stub = SourceGrpc.newStub(inProcessChannel);

        // Create a handshake request
        SourceOuterClass.ReadRequest handshakeRequest = SourceOuterClass.ReadRequest.newBuilder()
                .setHandshake(SourceOuterClass.Handshake.newBuilder().setSot(true).build())
                .build();

        // Test readFn, source has 10 messages
        // we read 5 messages, ack them, then read another 5 messages
        SourceOuterClass.ReadRequest request = SourceOuterClass.ReadRequest.newBuilder()
                .setRequest(SourceOuterClass.ReadRequest.Request
                        .newBuilder()
                        .setNumRecords(5)
                        .setTimeoutInMs(1000)
                        .build())
                .build();
        List<SourceOuterClass.AckRequest> ackRequests = new ArrayList<>();
        List<SourceOuterClass.Offset> offsets = new ArrayList<>();
        StreamObserver<SourceOuterClass.ReadRequest> readRequestObserver = stub.readFn(new StreamObserver<>() {
            int count = 0;
            boolean handshake = false;
            boolean eot = false;
            ArrayList<Map<String, MetadataOuterClass.KeyValueGroup>> userMetadataList = new ArrayList<>();

            @Override
            public void onNext(SourceOuterClass.ReadResponse readResponse) {
                // Handle handshake response
                if (readResponse.hasHandshake() && readResponse.getHandshake().getSot()) {
                    handshake = true;
                    return;
                }
                if (readResponse.getStatus().getEot()) {
                    eot = true;
                    return;
                }
                count++;
                userMetadataList.add(readResponse.getResult().getMetadata().getUserMetadataMap());
                SourceOuterClass.Offset offset = readResponse.getResult().getOffset();
                offsets.add(offset);
                SourceOuterClass.AckRequest.Request ackRequest = SourceOuterClass.AckRequest
                        .newBuilder()
                        .getRequest()
                        .toBuilder()
                        .addOffsets(offset)
                        .build();
                ackRequests.add(SourceOuterClass.AckRequest
                        .newBuilder()
                        .setRequest(ackRequest)
                        .build());
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {
                // we should have read 10 messages
                assertEquals(10, count);
                assertTrue(handshake);
                assertTrue(eot);
                // we should get 10 userMetadata with metadata intact
                assertEquals(10, userMetadataList.size());
                assertEquals(
                        "src-value",
                        userMetadataList.get(0)
                                .get("src-group").getKeyValueMap()
                                .get("src-key").toStringUtf8()
                );
            }
        });

        // Send handshake request
        readRequestObserver.onNext(handshakeRequest);

        // Send other read requests
        readRequestObserver.onNext(request);

        List<SourceOuterClass.AckResponse> ackResponses = new ArrayList<>();
        StreamObserver<SourceOuterClass.AckRequest> ackRequestObserver = stub.ackFn(new StreamObserver<>() {
            boolean handshake = false;
            int count = 0;

            @Override
            public void onNext(SourceOuterClass.AckResponse ackResponse) {
                if (ackResponse.hasHandshake() && ackResponse.getHandshake().getSot()) {
                    handshake = true;
                    return;
                }
                count++;
                ackResponses.add(ackResponse);
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onCompleted() {
                assertEquals(5, count);
                assertTrue(handshake);
            }
        });

        // Send handshake request
        ackRequestObserver.onNext(SourceOuterClass.AckRequest.newBuilder()
                .setHandshake(SourceOuterClass.Handshake.newBuilder().setSot(true).build())
                .build());

        // Send other ack requests
        ackRequests.forEach(ackRequestObserver::onNext);

        // get pending messages
        stub.pendingFn(Empty.newBuilder().build(), new StreamObserver<>() {
            @Override
            public void onNext(SourceOuterClass.PendingResponse pendingResponse) {
                assertEquals(5, pendingResponse.getResult().getCount());
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onCompleted() {
            }
        });

        readRequestObserver.onNext(request);

        // get partitions
        stub.partitionsFn(Empty.newBuilder().build(), new StreamObserver<>() {
            @Override
            public void onNext(SourceOuterClass.PartitionsResponse partitionsResponse) {
                assertEquals(1, partitionsResponse.getResult().getPartitionsCount());
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onCompleted() {
            }
        });

        // Nack the last 5 messages (indices 5-9)
        List<SourceOuterClass.Offset> nackedOffsets = offsets.subList(5, 10);
        stub.nackFn(SourceOuterClass.NackRequest.newBuilder()
                .setRequest(SourceOuterClass.NackRequest.Request.newBuilder()
                        .addAllOffsets(nackedOffsets)
                        .build())
                .build(), new StreamObserver<>() {
            @Override
            public void onNext(SourceOuterClass.NackResponse nackResponse) {
                // Verify nack was successful
                assertTrue(nackResponse.hasResult());
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onCompleted() {
            }
        });

        // After nacking, read again to verify we get the nacked messages back
        List<Integer> rereadOffsets = new ArrayList<>();
        StreamObserver<SourceOuterClass.ReadRequest> rereadRequestObserver = stub.readFn(new StreamObserver<>() {
            int count = 0;
            boolean handshake = false;

            @Override
            public void onNext(SourceOuterClass.ReadResponse readResponse) {
                // Handle handshake response
                if (readResponse.hasHandshake() && readResponse.getHandshake().getSot()) {
                    handshake = true;
                    return;
                }
                if (readResponse.getStatus().getEot()) {
                    return;
                }
                count++;
                // Decode the offset to verify it's one of the nacked messages
                SourceOuterClass.Offset offset = readResponse.getResult().getOffset();
                Integer decodedOffset = ByteBuffer.wrap(offset.getOffset().toByteArray()).getInt();
                rereadOffsets.add(decodedOffset);
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onCompleted() {
                // We should have read 5 nacked messages
                assertEquals(5, count);
                assertTrue(handshake);
                // Verify the offsets are the ones we nacked (5-9)
                for (int i = 0; i < 5; i++) {
                    assertEquals(Integer.valueOf(5 + i), rereadOffsets.get(i));
                }
            }
        });

        // Send handshake for the re-read
        rereadRequestObserver.onNext(handshakeRequest);
        // Request to read the nacked messages
        rereadRequestObserver.onNext(request);
        rereadRequestObserver.onCompleted();

        readRequestObserver.onCompleted();
        ackRequestObserver.onCompleted();
    }

    private static class TestSourcer extends Sourcer {
        List<Message> messages = new ArrayList<>();
        AtomicInteger readIndex = new AtomicInteger(0);
        Map<Integer, Boolean> yetToBeAcked = new ConcurrentHashMap<>();
        Map<Integer, Boolean> nacked = new ConcurrentHashMap<>();

        public TestSourcer() {
            Instant eventTime = Instant.ofEpochMilli(1000L);

            // create user metadata
            Map<String, Map<String, byte[]>> userMetadataMap = new HashMap<>();
            userMetadataMap.put("src-group", Map.of("src-key", "src-value".getBytes()));
            UserMetadata userMetadata = new UserMetadata(userMetadataMap);

            for (int i = 0; i < 10; i++) {
                messages.add(new Message(
                        ByteBuffer.allocate(4).putInt(i).array(),
                        new Offset(ByteBuffer.allocate(4).putInt(i).array(), 0),
                        eventTime,
                        userMetadata
                ));
                eventTime = eventTime.plusMillis(1000L);
            }
        }

        @Override
        public void read(ReadRequest request, OutputObserver observer) {
            if (!nacked.isEmpty()) {
                for (int i = 0; i < nacked.size(); i++) {
                    observer.send(messages.get(readIndex.get()));
                    yetToBeAcked.put(readIndex.get(), true);
                    readIndex.incrementAndGet();
                }
                nacked.clear();
            }

            if (readIndex.get() >= messages.size()) {
                return;
            }
            for (int i = 0; i < request.getCount(); i++) {
                if (readIndex.get() >= messages.size()) {
                    return;
                }
                observer.send(messages.get(readIndex.get()));
                yetToBeAcked.put(readIndex.get(), true);
                readIndex.incrementAndGet();
            }
        }

        @Override
        public List<Integer> getPartitions() {
            return Sourcer.defaultPartitions();
        }

        @Override
        public void ack(AckRequest request) {
            for (Offset offset : request.getOffsets()) {
                Integer decoded_offset = ByteBuffer.wrap(offset.getValue()).getInt();
                yetToBeAcked.remove(decoded_offset);
            }
        }

        @Override
        public void nack(NackRequest request) {
            for (Offset offset : request.getOffsets()) {
                Integer decoded_offset = ByteBuffer.wrap(offset.getValue()).getInt();
                yetToBeAcked.remove(decoded_offset);
                nacked.put(decoded_offset, true);
                readIndex.decrementAndGet();
            }
        }

        @Override
        public long getPending() {
            return messages.size() - readIndex.get();
        }
    }
}
