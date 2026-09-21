package io.numaproj.numaflow.sinker;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.shared.NackOptions;
import io.numaproj.numaflow.sink.v1.SinkGrpc;
import io.numaproj.numaflow.sink.v1.SinkOuterClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ServerNackTest {
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
        server = new Server(grpcServerConfig, new NackSinkFn(), null, serverName);
        server.start();
        inProcessChannel = grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void sinkerNack() {
        SinkOutputStreamObserver outputStreamObserver = new SinkOutputStreamObserver();
        StreamObserver<SinkOuterClass.SinkRequest> in =
                SinkGrpc.newStub(inProcessChannel).sinkFn(outputStreamObserver);

        in.onNext(SinkOuterClass.SinkRequest.newBuilder()
                .setHandshake(SinkOuterClass.Handshake.newBuilder().setSot(true).build()).build());
        in.onNext(SinkOuterClass.SinkRequest.newBuilder()
                .setRequest(SinkOuterClass.SinkRequest.Request.newBuilder()
                        .setValue(ByteString.copyFromUtf8("x")).setId("nack-1").build()).build());
        in.onNext(SinkOuterClass.SinkRequest.newBuilder()
                .setStatus(SinkOuterClass.TransmissionStatus.newBuilder().setEot(true).build()).build());
        in.onCompleted();

        while (!outputStreamObserver.completed.get()) {
            // busy-wait, matching the existing sinker ServerTest pattern
        }
        List<SinkOuterClass.SinkResponse> responses = outputStreamObserver.getSinkResponse();
        SinkOuterClass.SinkResponse.Result r = responses.stream()
                .flatMap(resp -> resp.getResultsList().stream())
                .filter(res -> res.getId().equals("nack-1"))
                .findFirst().orElseThrow(() -> new AssertionError("no result for nack-1"));
        assertEquals(SinkOuterClass.Status.NACK, r.getStatus());
        assertTrue(r.hasNackOptions());
        assertEquals(500L, r.getNackOptions().getDelay());
        assertEquals("retry", r.getNackOptions().getReason());
    }

    private static class NackSinkFn extends Sinker {
        @Override
        public ResponseList processMessages(DatumIterator datumStream) {
            ResponseList.ResponseListBuilder builder = ResponseList.newBuilder();
            while (true) {
                Datum datum;
                try {
                    datum = datumStream.next();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    continue;
                }
                if (datum == null) {
                    break;
                }
                builder.addResponse(Response.responseNack(
                        datum.getId(),
                        NackOptions.newBuilder().delay(500L).reason("retry").build()));
            }
            return builder.build();
        }
    }
}
