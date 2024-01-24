package io.numaproj.numaflow.sessionreducer;

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
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.sessionreduce.v1.SessionReduceGrpc;
import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;
import io.numaproj.numaflow.sessionreducer.model.Datum;
import io.numaproj.numaflow.sessionreducer.model.Message;
import io.numaproj.numaflow.sessionreducer.model.OutputStreamObserver;
import io.numaproj.numaflow.sessionreducer.model.SessionReducer;
import io.numaproj.numaflow.sessionreducer.model.SessionReducerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ServerTest {
    private final static String REDUCE_PROCESSED_KEY_SUFFIX = "-processed";
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private Server server;
    private ManagedChannel inProcessChannel;

    @Before
    public void setUp() throws Exception {

        ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                final var context = Context.current();
                return Contexts.interceptCall(context, call, headers, next);
            }
        };

        String serverName = InProcessServerBuilder.generateName();

        GRPCConfig grpcServerConfig = GRPCConfig.newBuilder()
                .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
                .socketPath(Constants.DEFAULT_SOCKET_PATH)
                .infoFilePath("/tmp/numaflow-test-server-info)")
                .build();

        server = new Server(
                new SessionReducerTestFactory(),
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

        // create an output stream observer
        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();

        StreamObserver<Sessionreduce.SessionReduceRequest> inputStreamObserver = SessionReduceGrpc
                .newStub(inProcessChannel)
                .sessionReduceFn(outputStreamObserver);

        for (int i = 1; i <= 11; i++) {
            Sessionreduce.SessionReduceRequest request = Sessionreduce.SessionReduceRequest
                    .newBuilder()
                    .setPayload(Sessionreduce.SessionReduceRequest.Payload
                            .newBuilder()
                            .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                            .addAllKeys(Arrays.asList(reduceKey))
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
        int keyCount = 3;

        // create an output stream observer
        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();

        StreamObserver<Sessionreduce.SessionReduceRequest> inputStreamObserver = SessionReduceGrpc
                .newStub(inProcessChannel)
                .sessionReduceFn(outputStreamObserver);

        // send messages with keyCount different keys
        for (int j = 0; j < keyCount; j++) {
            for (int i = 1; i <= 11; i++) {
                Sessionreduce.SessionReduceRequest request = Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(Arrays.asList(reduceKey + j))
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
        List<Sessionreduce.SessionReduceResponse> result = outputStreamObserver.resultDatum.get();
        // the outputStreamObserver should have observed 3*keyCount responses, 2 with real output sum data, one as EOF.
        assertEquals(keyCount * 3, result.size());
        result.forEach(response -> {
            assertTrue(response.getResult().getValue().equals(expectedFirstResponse) ||
                    response.getResult().getValue().equals(expectedSecondResponse)
                    || response.getEOF());

        });
    }

    public static class SessionReducerTestFactory extends SessionReducerFactory<SessionReducerTestFactory.TestSessionReducerHandler> {
        @Override
        public TestSessionReducerHandler createSessionReducer() {
            return new TestSessionReducerHandler();
        }

        public static class TestSessionReducerHandler extends SessionReducer {
            private int sum = 0;

            @Override
            public void processMessage(
                    String[] keys,
                    Datum datum,
                    OutputStreamObserver outputStreamObserver) {
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
                    OutputStreamObserver outputStreamObserver) {
                String[] updatedKeys = Arrays
                        .stream(keys)
                        .map(c -> c + "-processed")
                        .toArray(String[]::new);
                Message message = new Message(String.valueOf(sum).getBytes(), updatedKeys);
                outputStreamObserver.send(message);
            }

            @Override
            public byte[] accumulator() {
                return new byte[0];
            }

            @Override
            public void mergeAccumulator(byte[] accumulator) {
            }
        }
    }
}
