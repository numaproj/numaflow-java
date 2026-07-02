package io.numaproj.numaflow.sourcer;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.shared.NackOptions;
import io.numaproj.numaflow.source.v1.SourceGrpc;
import io.numaproj.numaflow.source.v1.SourceOuterClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ServerNackOptionsTest {
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private Server server;
    private ManagedChannel inProcessChannel;
    private final AtomicReference<NackRequest> captured = new AtomicReference<>();

    @Before
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        GRPCConfig grpcServerConfig = GRPCConfig.newBuilder()
                .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
                .socketPath(Constants.DEFAULT_SOCKET_PATH)
                .infoFilePath("/tmp/numaflow-test-server-info)")
                .build();
        server = new Server(grpcServerConfig, new CapturingSourcer(captured), null, serverName);
        server.start();
        inProcessChannel = grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void nackFnForwardsOptions() throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        SourceOuterClass.NackRequest req = SourceOuterClass.NackRequest.newBuilder()
                .addRequest(SourceOuterClass.NackRequest.Request.newBuilder()
                        .addOffsets(SourceOuterClass.Offset.newBuilder()
                                .setOffset(com.google.protobuf.ByteString.copyFromUtf8("o1"))
                                .setPartitionId(0).build())
                        .setNackOptions(common.NackOptionsOuterClass.NackOptions.newBuilder()
                                .setDelay(500L).setMaxDeliveries(3).setReason("retry").build())
                        .build())
                .build();
        SourceGrpc.newStub(inProcessChannel).nackFn(req, new StreamObserver<>() {
            @Override public void onNext(SourceOuterClass.NackResponse v) { }
            @Override public void onError(Throwable t) { done.countDown(); }
            @Override public void onCompleted() { done.countDown(); }
        });
        assertEquals(true, done.await(5, TimeUnit.SECONDS));

        NackRequest got = captured.get();
        assertNotNull(got);
        NackOptions opts = got.getOffsets().get(0).getNackOptions();
        assertNotNull(opts);
        assertEquals(Long.valueOf(500L), opts.getDelay());
        assertEquals(Integer.valueOf(3), opts.getMaxDeliveries());
        assertEquals("retry", opts.getReason());
    }

    private static class CapturingSourcer extends Sourcer {
        private final AtomicReference<NackRequest> sink;
        CapturingSourcer(AtomicReference<NackRequest> sink) { this.sink = sink; }
        @Override public void read(ReadRequest request, OutputObserver observer) { }
        @Override public void ack(AckRequest request) { }
        @Override public void nack(NackRequest request) { sink.set(request); }
        @Override public long getPending() { return 0; }
        @Override public List<Integer> getActivePartitions() { return java.util.Collections.emptyList(); }
        @Override public Integer getTotalPartitions() { return null; }
    }
}
