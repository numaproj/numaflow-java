package io.numaproj.numaflow.reducer;

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
import io.numaproj.numaflow.shared.GrpcServerUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static io.numaproj.numaflow.shared.GrpcServerUtils.WIN_END_KEY;
import static io.numaproj.numaflow.shared.GrpcServerUtils.WIN_START_KEY;
import static org.junit.Assert.assertEquals;

public class ServerTest {
    private final static String REDUCE_PROCESSED_KEY_SUFFIX = "-processed";

    public static final Metadata.Key<String> DATUM_METADATA_WIN_START = io.grpc.Metadata.Key.of(
            WIN_START_KEY,
            Metadata.ASCII_STRING_MARSHALLER);


    public static final Metadata.Key<String> DATUM_METADATA_WIN_END = Metadata.Key.of(
            WIN_END_KEY,
            Metadata.ASCII_STRING_MARSHALLER);


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

        GRPCConfig grpcServerConfig = GRPCConfig.newBuilder()
                .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
                .socketPath(Constants.DEFAULT_SOCKET_PATH)
                .infoFilePath("/tmp/numaflow-test-server-info)")
                .build();

        server = new Server( new ReduceTestFactory(),
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
    public void TestReducerWithOneKey() {
        String reduceKey = "reduce-key";



        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of(WIN_START_KEY, Metadata.ASCII_STRING_MARSHALLER), "60000");
        metadata.put(Metadata.Key.of(WIN_END_KEY, Metadata.ASCII_STRING_MARSHALLER), "120000");

        //create an output stream observer
        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();

        StreamObserver<ReduceOuterClass.ReduceRequest> inputStreamObserver = ReduceGrpc
                .newStub(inProcessChannel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                .reduceFn(outputStreamObserver);

        for (int i = 1; i <= 10; i++) {
            ReduceOuterClass.ReduceRequest request = ReduceOuterClass.ReduceRequest.newBuilder()
                    .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                    .addKeys(reduceKey)
                    .build();
            inputStreamObserver.onNext(request);
        }

        inputStreamObserver.onCompleted();

        String[] expectedKeys = new String[]{reduceKey + REDUCE_PROCESSED_KEY_SUFFIX};
        // sum of first 10 numbers 1 to 10 -> 55
        ByteString expectedValue = ByteString.copyFromUtf8(String.valueOf(55));
        while (!outputStreamObserver.completed.get()) ;

        assertEquals(1, outputStreamObserver.resultDatum.get().getResultsCount());
        assertEquals(
                expectedKeys,
                outputStreamObserver.resultDatum
                        .get()
                        .getResults(0)
                        .getKeysList()
                        .toArray(new String[0]));
        assertEquals(
                expectedValue,
                outputStreamObserver.resultDatum.get().getResults(0).getValue());
    }

        @Test
    public void TestReducerWithMultipleKey() {
        String reduceKey = "reduce-key";
        int keyCount = 100;

        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of(WIN_START_KEY, Metadata.ASCII_STRING_MARSHALLER), "60000");
        metadata.put(Metadata.Key.of(WIN_END_KEY, Metadata.ASCII_STRING_MARSHALLER), "120000");

        //create an output stream observer
        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();

        StreamObserver<ReduceOuterClass.ReduceRequest> inputStreamObserver = ReduceGrpc
                .newStub(inProcessChannel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                .reduceFn(outputStreamObserver);

        // send messages with 100 different keys
        for (int j = 0; j < keyCount; j++) {
            for (int i = 1; i <= 10; i++) {
                ReduceOuterClass.ReduceRequest inputDatum = ReduceOuterClass.ReduceRequest.newBuilder()
                        .addKeys(reduceKey + j)
                        .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                        .build();
                inputStreamObserver.onNext(inputDatum);
            }
        }

        inputStreamObserver.onCompleted();

        // sum of first 10 numbers 1 to 10 -> 55
        ByteString expectedValue = ByteString.copyFromUtf8(String.valueOf(55));

        while (!outputStreamObserver.completed.get()) ;
        ReduceOuterClass.ReduceResponse result = outputStreamObserver.resultDatum.get();
        assertEquals(100, result.getResultsCount());
        for (int i = 0; i < keyCount; i++) {
            assertEquals(expectedValue, result.getResults(0).getValue());
        }
    }
}
