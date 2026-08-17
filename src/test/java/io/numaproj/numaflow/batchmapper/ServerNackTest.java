package io.numaproj.numaflow.batchmapper;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.map.v1.MapGrpc;
import io.numaproj.numaflow.map.v1.MapOuterClass;
import io.numaproj.numaflow.shared.NackOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
        server = new Server(grpcServerConfig, new NackBatchMapFn(), null, serverName);
        server.start();
        inProcessChannel = grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void batchMapNack() {
        // expect: handshake resp + 1 per-id response + 1 EOT = 3
        BatchMapOutputStreamObserver outputStreamObserver = new BatchMapOutputStreamObserver(3);
        StreamObserver<MapOuterClass.MapRequest> in = MapGrpc.newStub(inProcessChannel)
                .mapFn(outputStreamObserver);
        in.onNext(MapOuterClass.MapRequest.newBuilder()
                .setHandshake(MapOuterClass.Handshake.newBuilder().setSot(true)).build());
        in.onNext(MapOuterClass.MapRequest.newBuilder()
                .setRequest(MapOuterClass.MapRequest.Request.newBuilder()
                        .setValue(ByteString.copyFromUtf8("x")).addKeys("k").build())
                .setId("id-1").build());
        in.onNext(MapOuterClass.MapRequest.newBuilder()
                .setStatus(MapOuterClass.TransmissionStatus.newBuilder().setEot(true)).build());
        in.onCompleted();
        try {
            outputStreamObserver.done.get();
        } catch (InterruptedException | ExecutionException e) {
            fail("Error in getting done signal " + e.getMessage());
        }
        List<MapOuterClass.MapResponse> result = outputStreamObserver.getMapResponses();
        MapOuterClass.MapResponse.Result r = result.stream()
                .filter(resp -> resp.getResultsCount() > 0)
                .findFirst().orElseThrow(() -> new AssertionError("no result")).getResults(0);
        assertEquals(Arrays.asList("U+005C__NACK__"), r.getTagsList());
        assertTrue(r.hasNackOptions());
        assertEquals(500L, r.getNackOptions().getDelay());
        assertEquals("retry", r.getNackOptions().getReason());
    }

    private static class NackBatchMapFn extends BatchMapper {
        @Override
        public BatchResponses processMessage(DatumIterator datumStream) {
            BatchResponses batchResponses = new BatchResponses();
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
                BatchResponse batchResponse = new BatchResponse(datum.getId());
                batchResponse.append(Message.toNack(NackOptions.newBuilder().delay(500L).reason("retry").build()));
                batchResponses.append(batchResponse);
            }
            return batchResponses;
        }
    }
}
