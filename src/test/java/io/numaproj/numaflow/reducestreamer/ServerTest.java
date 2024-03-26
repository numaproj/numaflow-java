package io.numaproj.numaflow.reducestreamer;

import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.reduce.v1.ReduceGrpc;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import io.numaproj.numaflow.reducestreamer.model.Datum;
import io.numaproj.numaflow.reducestreamer.model.Message;
import io.numaproj.numaflow.reducestreamer.model.OutputStreamObserver;
import io.numaproj.numaflow.reducestreamer.model.ReduceStreamer;
import io.numaproj.numaflow.reducestreamer.model.ReduceStreamerFactory;
import io.numaproj.numaflow.shared.GrpcServerUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.numaproj.numaflow.shared.GrpcServerUtils.WIN_END_KEY;
import static io.numaproj.numaflow.shared.GrpcServerUtils.WIN_START_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ServerTest {
    public static final Metadata.Key<String> DATUM_METADATA_WIN_START = Metadata.Key.of(
            WIN_START_KEY,
            Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> DATUM_METADATA_WIN_END = Metadata.Key.of(
            WIN_END_KEY,
            Metadata.ASCII_STRING_MARSHALLER);
    private final static String REDUCE_PROCESSED_KEY_SUFFIX = "-processed";
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private io.numaproj.numaflow.reducestreamer.Server server;
    private ManagedChannel inProcessChannel;

    @Before
    public void setUp() throws Exception {

        ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                final var context =
                        Context.current().withValues(
                                GrpcServerUtils.WINDOW_START_TIME,
                                headers.get(DATUM_METADATA_WIN_START),
                                GrpcServerUtils.WINDOW_END_TIME,
                                headers.get(DATUM_METADATA_WIN_END));
                return Contexts.interceptCall(context, call, headers, next);
            }
        };

        String serverName = InProcessServerBuilder.generateName();

        io.numaproj.numaflow.reducestreamer.GRPCConfig grpcServerConfig = GRPCConfig.newBuilder()
                .maxMessageSize(io.numaproj.numaflow.reducestreamer.Constants.DEFAULT_MESSAGE_SIZE)
                .socketPath(io.numaproj.numaflow.reducestreamer.Constants.DEFAULT_SOCKET_PATH)
                .infoFilePath("/tmp/numaflow-test-server-info)")
                .build();

        server = new Server(
                new ReduceStreamerTestFactory(),
                grpcServerConfig);

        server.setServerBuilder(InProcessServerBuilder.forName(serverName)
                .intercept(interceptor)
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
    public void given_inputReduceRequestsShareSameKey_when_serverStarts_then_allRequestsGetAggregatedToOneResponse() {
        String reduceKey = "reduce-key";

        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of(WIN_START_KEY, Metadata.ASCII_STRING_MARSHALLER), "60000");
        metadata.put(Metadata.Key.of(WIN_END_KEY, Metadata.ASCII_STRING_MARSHALLER), "120000");

        // create an output stream observer
        io.numaproj.numaflow.reducer.ReduceOutputStreamObserver outputStreamObserver = new io.numaproj.numaflow.reducer.ReduceOutputStreamObserver();

        StreamObserver<ReduceOuterClass.ReduceRequest> inputStreamObserver = ReduceGrpc
                .newStub(inProcessChannel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                .reduceFn(outputStreamObserver);

        for (int i = 1; i <= 11; i++) {
            ReduceOuterClass.ReduceRequest request = ReduceOuterClass.ReduceRequest.newBuilder()
                    .setPayload(ReduceOuterClass.ReduceRequest.Payload
                            .newBuilder()
                            .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                            .addAllKeys(List.of(reduceKey))
                            .build())
                    .build();
            inputStreamObserver.onNext(request);
        }

        inputStreamObserver.onCompleted();

        String[] expectedKeys = new String[]{reduceKey + REDUCE_PROCESSED_KEY_SUFFIX};
        // sum of first 10 numbers 1 to 10 -> 55
        ByteString expectedFirstResponse = ByteString.copyFromUtf8(String.valueOf(55));
        // after the sum reaches 55, the test reducer reset the sum, hence when EOF is sent from input stream, the sum is 11 and gets sent to output stream.
        ByteString expectedSecondResponse = ByteString.copyFromUtf8(String.valueOf(11));
        while (!outputStreamObserver.completed.get()) ;

        // Expect 2 responses, one containing the aggregated data and the other indicating EOF.
        assertEquals(3, outputStreamObserver.resultDatum.get().size());
        assertEquals(
                expectedKeys,
                outputStreamObserver.resultDatum
                        .get()
                        .get(0)
                        .getResult()
                        .getKeysList()
                        .toArray(new String[0]));
        assertEquals(
                expectedFirstResponse,
                outputStreamObserver.resultDatum
                        .get()
                        .get(0)
                        .getResult()
                        .getValue());
        assertEquals(
                expectedKeys,
                outputStreamObserver.resultDatum
                        .get()
                        .get(1)
                        .getResult()
                        .getKeysList()
                        .toArray(new String[0]));
        assertEquals(
                expectedSecondResponse,
                outputStreamObserver.resultDatum
                        .get()
                        .get(1)
                        .getResult()
                        .getValue());
        assertTrue(outputStreamObserver.resultDatum.get().get(2).getEOF());
    }

    @Test
    public void given_inputReduceRequestsHaveDifferentKeySets_when_serverStarts_then_requestsGetAggregatedSeparately() {
        String reduceKey = "reduce-key";
        int keyCount = 10;

        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of(WIN_START_KEY, Metadata.ASCII_STRING_MARSHALLER), "60000");
        metadata.put(Metadata.Key.of(WIN_END_KEY, Metadata.ASCII_STRING_MARSHALLER), "120000");

        // create an output stream observer
        io.numaproj.numaflow.reducestreamer.ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();

        StreamObserver<ReduceOuterClass.ReduceRequest> inputStreamObserver = ReduceGrpc
                .newStub(inProcessChannel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                .reduceFn(outputStreamObserver);

        // send messages with keyCount different keys
        for (int j = 0; j < keyCount; j++) {
            for (int i = 1; i <= 11; i++) {
                ReduceOuterClass.ReduceRequest request = ReduceOuterClass.ReduceRequest
                        .newBuilder()
                        .setPayload(ReduceOuterClass.ReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of(reduceKey + j))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                                .build())
                        .build();
                inputStreamObserver.onNext(request);
            }
        }

        inputStreamObserver.onCompleted();

        // sum of first 10 numbers 1 to 10 -> 55
        ByteString expectedFirstResponse = ByteString.copyFromUtf8(String.valueOf(55));
        // after the sum reaches 55, the test reducer reset the sum, hence when EOF is sent from input stream, the sum is 11 and gets sent to output stream.
        ByteString expectedSecondResponse = ByteString.copyFromUtf8(String.valueOf(11));

        while (!outputStreamObserver.completed.get()) ;
        List<ReduceOuterClass.ReduceResponse> result = outputStreamObserver.resultDatum.get();
        // the outputStreamObserver should have observed (keyCount * 2 + 1) responses, 2 with real output sum data per key, 1 as the final single EOF response.
        assertEquals(keyCount * 2 + 1, result.size());
        for (int i = 0; i < keyCount * 2; i++) {
            ReduceOuterClass.ReduceResponse response = result.get(i);
            assertTrue(response.getResult().getValue().equals(expectedFirstResponse) ||
                    response.getResult().getValue().equals(expectedSecondResponse));
        }
        // verify the last one is the EOF.
        assertTrue(result.get(keyCount * 2).getEOF());
    }

    public static class ReduceStreamerTestFactory extends ReduceStreamerFactory<ServerTest.ReduceStreamerTestFactory.TestReduceStreamHandler> {
        @Override
        public ServerTest.ReduceStreamerTestFactory.TestReduceStreamHandler createReduceStreamer() {
            return new ServerTest.ReduceStreamerTestFactory.TestReduceStreamHandler();
        }

        public static class TestReduceStreamHandler extends ReduceStreamer {
            private int sum = 0;

            @Override
            public void processMessage(
                    String[] keys,
                    Datum datum,
                    OutputStreamObserver outputStreamObserver,
                    io.numaproj.numaflow.reducestreamer.model.Metadata md) {
                sum += Integer.parseInt(new String(datum.getValue()));
                if (sum > 50) {
                    String[] updatedKeys = Arrays
                            .stream(keys)
                            .map(c -> c + "-processed")
                            .toArray(String[]::new);
                    Message message = new Message(String.valueOf(sum).getBytes(), updatedKeys);
                    outputStreamObserver.send(message);
                    // reset sum
                    sum = 0;
                }
            }

            @Override
            public void handleEndOfStream(
                    String[] keys,
                    OutputStreamObserver outputStreamObserver,
                    io.numaproj.numaflow.reducestreamer.model.Metadata md) {
                String[] updatedKeys = Arrays
                        .stream(keys)
                        .map(c -> c + "-processed")
                        .toArray(String[]::new);
                Message message = new Message(String.valueOf(sum).getBytes(), updatedKeys);
                outputStreamObserver.send(message);
            }
        }
    }
}
