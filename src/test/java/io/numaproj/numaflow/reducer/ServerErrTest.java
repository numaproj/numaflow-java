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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static io.numaproj.numaflow.reducer.Constants.WIN_END_KEY;
import static io.numaproj.numaflow.reducer.Constants.WIN_START_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ServerErrTest {


    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private Server server;
    private ManagedChannel inProcessChannel;

    public static final Metadata.Key<String> DATUM_METADATA_WIN_START = io.grpc.Metadata.Key.of(
            WIN_START_KEY,
            Metadata.ASCII_STRING_MARSHALLER);


    public static final Metadata.Key<String> DATUM_METADATA_WIN_END = Metadata.Key.of(
            WIN_END_KEY,
            Metadata.ASCII_STRING_MARSHALLER);

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
                                Constants.WINDOW_START_TIME,
                                headers.get(DATUM_METADATA_WIN_START),
                                Constants.WINDOW_END_TIME,
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

        server = new Server( new ReduceErrTestFactory(),
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
    public void TestReducerErr() {
        String reduceKey = "reduce-key";

        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of(WIN_START_KEY, Metadata.ASCII_STRING_MARSHALLER), "60000");
        metadata.put(Metadata.Key.of(WIN_END_KEY, Metadata.ASCII_STRING_MARSHALLER), "120000");

        //create an output stream observer
        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();

        Thread t = new Thread(() -> {
            while (outputStreamObserver.t == null){
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            assertEquals("UNKNOWN: java.lang.RuntimeException: unknown exception", outputStreamObserver.t.getMessage());
        });
        t.start();

        StreamObserver<ReduceOuterClass.ReduceRequest> inputStreamObserver = ReduceGrpc
                .newStub(inProcessChannel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                .reduceFn(outputStreamObserver);

        for (int i = 1; i <= 10; i++) {
            ReduceOuterClass.ReduceRequest reduceRequest = ReduceOuterClass.ReduceRequest.newBuilder()
                    .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                    .addKeys(reduceKey)
                    .build();
            inputStreamObserver.onNext(reduceRequest);
        }

        inputStreamObserver.onCompleted();

        try {
            t.join();
        } catch (InterruptedException e) {
            fail("Thread interrupted");
        }
    }
}
