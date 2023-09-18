package io.numaproj.numaflow.sourcer;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.source.v1.SourceGrpc;
import io.numaproj.numaflow.source.v1.SourceOuterClass;
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
        var stub = SourceGrpc.newBlockingStub(inProcessChannel);

        // Test readFn, source has 10 messages
        // we read 5 messages, ack them, then read another 5 messages
        SourceOuterClass.ReadRequest request = SourceOuterClass.ReadRequest.newBuilder()
                .setRequest(SourceOuterClass.ReadRequest.Request
                        .newBuilder()
                        .setNumRecords(5)
                        .setTimeoutInMs(1000)
                        .build())
                .build();

        var response = stub.readFn(request);

        SourceOuterClass.AckRequest.Builder ackRequestBuilder = SourceOuterClass.AckRequest.newBuilder();
        int count = 0;
        while (response.hasNext()) {
            var message = response.next();
            count++;
            SourceOuterClass.Offset offset = message.getResult().getOffset();
            ackRequestBuilder.getRequest().toBuilder().addOffsets(offset);
        }

        // we should have read 5 messages
        assertEquals(5, count);

        // since total messages is 10, we should have 5 pending
        var pending = stub.pendingFn(Empty.newBuilder().build());
        assertEquals(5, pending.getResult().getCount());

        // ack the 5 messages
        var ackResponse = stub.ackFn(ackRequestBuilder.build());
        assertEquals(Empty.newBuilder().build(), ackResponse.getResult().getSuccess());

        // read another 5 messages
        request = SourceOuterClass.ReadRequest.newBuilder()
                .setRequest(SourceOuterClass.ReadRequest.Request
                        .newBuilder()
                        .setNumRecords(5)
                        .setTimeoutInMs(1000)
                        .build())
                .build();

        response = stub.readFn(request);

        ackRequestBuilder = SourceOuterClass.AckRequest.newBuilder();
        while (response.hasNext()) {
            var message = response.next();
            count++;
            SourceOuterClass.Offset offset = message.getResult().getOffset();
            ackRequestBuilder.getRequest().toBuilder().addOffsets(offset);
        }

        // we should have read 5 messages
        assertEquals(10, count);

        // since total messages is 10, we should have 0 pending
        pending = stub.pendingFn(Empty.newBuilder().build());
        assertEquals(0, pending.getResult().getCount());

        // ack the 5 messages
        ackResponse = stub.ackFn(ackRequestBuilder.build());
        assertEquals(Empty.newBuilder().build(), ackResponse.getResult().getSuccess());
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
                        new Offset(ByteBuffer.allocate(4).putInt(i).array(), "0"),
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
        public void ack(AckRequest request) {
            for (Offset offset : request.getOffsets()) {
                yetToBeAcked.remove(ByteBuffer.wrap(offset.getValue()).getInt());
            }
        }

        @Override
        public long getPending() {
            return messages.size() - readIndex.get();
        }
    }
}
