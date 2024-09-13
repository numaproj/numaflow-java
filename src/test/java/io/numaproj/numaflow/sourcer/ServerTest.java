package io.numaproj.numaflow.sourcer;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.source.v1.SourceGrpc;
import io.numaproj.numaflow.source.v1.SourceOuterClass;
import org.checkerframework.checker.units.qual.A;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;


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
                new TestSourcer(),
                grpcServerConfig);

        server.setServerBuilder(InProcessServerBuilder.forName(serverName)
                .directExecutor());

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

        StreamObserver<SourceOuterClass.ReadRequest> readRequestObserver  = stub.readFn(new StreamObserver<SourceOuterClass.ReadResponse>() {
            int count = 0;

            @Override
            public void onNext(SourceOuterClass.ReadResponse readResponse) {
                count++;
                SourceOuterClass.Offset offset = readResponse.getResult().getOffset();
                SourceOuterClass.AckRequest.Request ackRequest = SourceOuterClass.AckRequest.newBuilder().getRequest().toBuilder().setOffset(offset).build();
                ackRequests.add(SourceOuterClass.AckRequest.newBuilder().setRequest(ackRequest).build());
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {
                // we should have read 10 messages and 2 eot messages
                assertEquals(12, count);
            }
        });

        readRequestObserver.onNext(request);

        StreamObserver<SourceOuterClass.AckRequest> ackRequestObserver = stub.ackFn(new StreamObserver<SourceOuterClass.AckResponse>() {
            @Override
            public void onNext(SourceOuterClass.AckResponse ackResponse) {}

            @Override
            public void onError(Throwable throwable) {}

            @Override
            public void onCompleted() {}
        });

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

        readRequestObserver.onCompleted();
        ackRequestObserver.onCompleted();
    }

    private static class TestSourcer extends Sourcer {
        List<Message> messages = new ArrayList<>();
        AtomicInteger readIndex = new AtomicInteger(0);
        Map<Integer, Boolean> yetToBeAcked = new ConcurrentHashMap<>();

        public TestSourcer() {
            Instant eventTime = Instant.ofEpochMilli(1000L);
            for (int i = 0; i < 10; i++) {
                messages.add(new Message(
                        ByteBuffer.allocate(4).putInt(i).array(),
                        new Offset(ByteBuffer.allocate(4).putInt(i).array(), 0),
                        eventTime
                ));
                eventTime = eventTime.plusMillis(1000L);
            }
        }

        @Override
        public void read(ReadRequest request, OutputObserver observer) {
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
            yetToBeAcked.remove(ByteBuffer.wrap(request.getOffset().getValue()).getInt());
        }

        @Override
        public long getPending() {
            return messages.size() - readIndex.get();
        }
    }
}
