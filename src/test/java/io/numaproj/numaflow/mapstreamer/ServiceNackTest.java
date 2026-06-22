package io.numaproj.numaflow.mapstreamer;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
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
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ServiceNackTest {
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private Service service;
    private ManagedChannel inProcessChannel;

    @Before
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        CompletableFuture<Void> shutdownSignal = new CompletableFuture<>();
        service = new Service(new NackMapStreamer(), shutdownSignal);
        grpcCleanup.register(InProcessServerBuilder.forName(serverName).directExecutor()
                .addService(service).build().start());
        inProcessChannel = grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());
    }

    @After
    public void tearDown() {
        inProcessChannel.shutdownNow();
    }

    @Test
    public void mapStreamerNack() {
        MapOuterClass.MapRequest handshake = MapOuterClass.MapRequest.newBuilder()
                .setHandshake(MapOuterClass.Handshake.newBuilder().setSot(true)).build();
        MapOuterClass.MapRequest inDatum = MapOuterClass.MapRequest.newBuilder()
                .setRequest(MapOuterClass.MapRequest.Request.newBuilder()
                        .setValue(ByteString.copyFromUtf8("x")).addKeys("k").build()).build();

        // expect: handshake resp + 1 result + 1 EOT = 3 responses
        MapStreamOutputStreamObserver responseObserver = new MapStreamOutputStreamObserver(3);
        var stub = MapGrpc.newStub(inProcessChannel);
        var requestStreamObserver = stub.mapFn(responseObserver);
        requestStreamObserver.onNext(handshake);
        requestStreamObserver.onNext(inDatum);
        try {
            responseObserver.done.get();
        } catch (Exception e) {
            fail("Error while waiting for response" + e.getMessage());
        }
        List<MapOuterClass.MapResponse> responses = responseObserver.getMapResponses();
        MapOuterClass.MapResponse.Result r = responses.stream()
                .filter(resp -> resp.getResultsCount() > 0)
                .findFirst().orElseThrow(() -> new AssertionError("no result")).getResults(0);
        assertEquals(Arrays.asList("U+005C__NACK__"), r.getTagsList());
        assertTrue(r.hasNackOptions());
        assertEquals(500L, r.getNackOptions().getDelay());
        assertEquals("retry", r.getNackOptions().getReason());
        requestStreamObserver.onCompleted();
    }

    private static class NackMapStreamer extends MapStreamer {
        @Override
        public void processMessage(String[] keys, Datum datum, OutputObserver outputObserver) {
            outputObserver.send(Message.toNack(NackOptions.newBuilder().delay(500L).reason("retry").build()));
        }
    }
}
