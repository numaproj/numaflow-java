package io.numaproj.numaflow.accumulator;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.accumulator.model.Accumulator;
import io.numaproj.numaflow.accumulator.model.AccumulatorFactory;
import io.numaproj.numaflow.accumulator.model.Datum;
import io.numaproj.numaflow.accumulator.model.Message;
import io.numaproj.numaflow.accumulator.model.OutputStreamObserver;
import io.numaproj.numaflow.accumulator.v1.AccumulatorGrpc;
import io.numaproj.numaflow.accumulator.v1.AccumulatorOuterClass;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ServerTest {
    private final static String PROCESSED_KEY_SUFFIX = "-key-processed";
    private final static String PROCESSED_VALUE_SUFFIX = "-value-processed";

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
                new TestAccumFactory(),
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
    public void testAccumulatorSingleKey() {
        AccumulatorOuterClass.AccumulatorRequest openRequest = AccumulatorOuterClass.AccumulatorRequest
                .newBuilder()
                .setPayload(AccumulatorOuterClass.Payload
                        .newBuilder()
                        .setValue(ByteString.copyFromUtf8("test-payload"))
                        .addAllKeys(List.of("test-accumulator"))
                        .build())
                .setOperation(AccumulatorOuterClass.AccumulatorRequest.WindowOperation
                        .newBuilder()
                        .setEvent(AccumulatorOuterClass.AccumulatorRequest.WindowOperation.Event.OPEN)
                        .build()).build();
        AccumulatorOuterClass.AccumulatorRequest appendRequest = AccumulatorOuterClass.AccumulatorRequest
                .newBuilder()
                .setPayload(AccumulatorOuterClass.Payload
                        .newBuilder()
                        .setValue(ByteString.copyFromUtf8("test-payload"))
                        .addAllKeys(List.of("test-accumulator"))
                        .build())
                .setOperation(AccumulatorOuterClass.AccumulatorRequest.WindowOperation
                        .newBuilder()
                        .setEvent(AccumulatorOuterClass.AccumulatorRequest.WindowOperation.Event.OPEN)
                        .build()).build();

        String[] expectedKeys = new String[]{"test-accumulator" + PROCESSED_KEY_SUFFIX};
        String[] expectedTags = new String[]{"test-tag"};
        ByteString expectedValue = ByteString.copyFromUtf8("test-payload" + PROCESSED_VALUE_SUFFIX);

        AccumulatorStreamObserver responseObserver = new AccumulatorStreamObserver(4);

        var stub = AccumulatorGrpc.newStub(inProcessChannel);
        var requestStreamObserver = stub
                .accumulateFn(responseObserver);

        requestStreamObserver.onNext(openRequest);
        requestStreamObserver.onNext(appendRequest);
        requestStreamObserver.onNext(appendRequest);

        requestStreamObserver.onCompleted();

        try {
            responseObserver.done.get();
        } catch (InterruptedException | ExecutionException e) {
            fail("Error while waiting for response" + e.getMessage());
        }

        List<AccumulatorOuterClass.AccumulatorResponse> responses = responseObserver.getResponses();
        assertEquals(4, responses.size());

        // 3 + 1 eof response
        for (AccumulatorOuterClass.AccumulatorResponse response : responses) {
            if (response.getEOF()) {
                continue;
            }
            assertEquals(expectedValue, response.getPayload().getValue());
            assertEquals(Arrays.asList(expectedKeys), response.getPayload().getKeysList());
            assertEquals(Arrays.asList(expectedTags), response.getTagsList());
        }
    }

    private static class TestAccumFn extends Accumulator {
        @Override
        public void processMessage(Datum datum, OutputStreamObserver outputStream) {
            Message message = new Message(datum);
            message.setKeys(new String[]{datum.getKeys()[0] + PROCESSED_KEY_SUFFIX});
            message.setTags(new String[]{"test-tag"});
            message.setValue((new String(datum.getValue())
                    + PROCESSED_VALUE_SUFFIX).getBytes());
            outputStream.send(message);
        }

        @Override
        public void handleEndOfStream(OutputStreamObserver outputStreamObserver) {
        }

    }

    private static class TestAccumFactory extends AccumulatorFactory<Accumulator> {
        @Override
        public Accumulator createAccumulator() {
            return new TestAccumFn();
        }

    }
}
