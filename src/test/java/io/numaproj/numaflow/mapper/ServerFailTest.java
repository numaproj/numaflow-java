package io.numaproj.numaflow.mapper;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.map.v1.MapGrpc;
import io.numaproj.numaflow.map.v1.MapOuterClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ServerFailTest {
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
        server = new Server(grpcServerConfig, new FailMapFn(), null, serverName);
        server.start();
        inProcessChannel = grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void mapperFail() {
        MapOuterClass.MapRequest handshake = MapOuterClass.MapRequest.newBuilder()
                .setHandshake(MapOuterClass.Handshake.newBuilder().setSot(true)).build();
        MapOuterClass.MapRequest inDatum = MapOuterClass.MapRequest.newBuilder()
                .setRequest(MapOuterClass.MapRequest.Request.newBuilder()
                        .setValue(ByteString.copyFromUtf8("x")).addKeys("k").build()).build();

        MapOutputStreamObserver responseObserver = new MapOutputStreamObserver(2);
        var stub = MapGrpc.newStub(inProcessChannel);
        var requestStreamObserver = stub.mapFn(responseObserver);
        requestStreamObserver.onNext(handshake);
        requestStreamObserver.onNext(inDatum);
        try {
            responseObserver.done.get();
        } catch (InterruptedException | ExecutionException e) {
            fail("Error while waiting for response" + e.getMessage());
        }
        List<MapOuterClass.MapResponse> responses = responseObserver.getMapResponses().subList(1, 2);
        MapOuterClass.MapResponse.Result r = responses.get(0).getResults(0);
        assertEquals(Arrays.asList("U+005C__FAIL__"), r.getTagsList());
        requestStreamObserver.onCompleted();
    }

    private static class FailMapFn extends Mapper {
        @Override
        public MessageList processMessage(String[] keys, Datum datum) {
            return MessageList.newBuilder()
                    .addMessage(Message.toFail())
                    .build();
        }
    }
}
