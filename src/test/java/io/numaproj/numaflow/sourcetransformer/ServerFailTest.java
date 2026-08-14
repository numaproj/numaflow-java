package io.numaproj.numaflow.sourcetransformer;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.sourcetransformer.v1.SourceTransformGrpc;
import io.numaproj.numaflow.sourcetransformer.v1.Sourcetransformer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ServerFailTest {
    private static final Instant TEST_EVENT_TIME = Instant.ofEpochMilli(1000L);

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
        server = new Server(grpcServerConfig, new FailTransformer(), null, serverName);
        server.start();
        inProcessChannel = grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void transformerFail() {
        Sourcetransformer.SourceTransformRequest handshake = Sourcetransformer.SourceTransformRequest.newBuilder()
                .setHandshake(Sourcetransformer.Handshake.newBuilder().setSot(true).build()).build();
        Sourcetransformer.SourceTransformRequest req = Sourcetransformer.SourceTransformRequest.newBuilder()
                .setRequest(Sourcetransformer.SourceTransformRequest.Request.newBuilder()
                        .setValue(ByteString.copyFromUtf8("x")).addKeys("k").build()).build();

        TransformerOutputStreamObserver responseObserver = new TransformerOutputStreamObserver(2);
        var stub = SourceTransformGrpc.newStub(inProcessChannel);
        var requestStreamObserver = stub.sourceTransformFn(responseObserver);
        requestStreamObserver.onNext(handshake);
        requestStreamObserver.onNext(req);
        try {
            responseObserver.done.get();
        } catch (InterruptedException | ExecutionException e) {
            fail("Error while waiting for response" + e.getMessage());
        }
        List<Sourcetransformer.SourceTransformResponse> responses = responseObserver.getResponses().subList(1, 2);
        Sourcetransformer.SourceTransformResponse.Result r = responses.get(0).getResults(0);
        assertEquals(Arrays.asList("U+005C__FAIL__"), r.getTagsList());
        assertEquals(TEST_EVENT_TIME.getEpochSecond(), r.getEventTime().getSeconds());
        requestStreamObserver.onCompleted();
    }

    private static class FailTransformer extends SourceTransformer {
        @Override
        public MessageList processMessage(String[] keys, Datum datum) {
            return MessageList.newBuilder()
                    .addMessage(Message.toFail(TEST_EVENT_TIME))
                    .build();
        }
    }
}
