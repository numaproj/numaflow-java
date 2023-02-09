package io.numaproj.numaflow.function;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.common.GrpcServerConfig;
import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.reduce.GroupBy;
import io.numaproj.numaflow.function.v1.Udfunction;
import io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static io.numaproj.numaflow.function.Function.DATUM_KEY;
import static io.numaproj.numaflow.function.Function.WIN_END_KEY;
import static io.numaproj.numaflow.function.Function.WIN_START_KEY;
import static org.junit.Assert.assertNotNull;

public class ReduceExceptionTest {

    public static class ExceptionGroupBy extends GroupBy {

        public ExceptionGroupBy(String key, Metadata metadata) {
            super(key, metadata);
        }

        @Override
        public void readMessage(Datum datum) {
            throw new RuntimeException("null");
        }

        @Override
        public Message[] getResult() {
            return new Message[0];
        }
    }

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private FunctionServer server;
    private ManagedChannel inProcessChannel;

    @Before
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();

        server = new FunctionServer(
                InProcessServerBuilder.forName(serverName).directExecutor(),
                new GrpcServerConfig(Function.SOCKET_PATH, Function.DEFAULT_MESSAGE_SIZE));

        server
                .registerReducer(ExceptionGroupBy.class)
                .start();

        inProcessChannel = grpcCleanup.register(InProcessChannelBuilder
                .forName(serverName)
                .directExecutor()
                .build());
    }

    @Test
    public void reducerWithExceptionTest() {
        String reduceKey = "reduce-key";
        Udfunction.Datum.Builder inDatumBuilder = Udfunction.Datum.newBuilder().setKey(reduceKey);

        io.grpc.Metadata metadata = new io.grpc.Metadata();
        metadata.put(io.grpc.Metadata.Key.of(DATUM_KEY, io.grpc.Metadata.ASCII_STRING_MARSHALLER), reduceKey);
        metadata.put(io.grpc.Metadata.Key.of(WIN_START_KEY, io.grpc.Metadata.ASCII_STRING_MARSHALLER), "60000");
        metadata.put(io.grpc.Metadata.Key.of(WIN_END_KEY, io.grpc.Metadata.ASCII_STRING_MARSHALLER), "120000");

        //create an output stream observer
        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();

        StreamObserver<Udfunction.Datum> inputStreamObserver = UserDefinedFunctionGrpc
                .newStub(inProcessChannel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                .reduceFn(outputStreamObserver);

        for (int i = 1; i <= 10; i++) {
            Udfunction.Datum inputDatum = inDatumBuilder
                    .setKey("reduce-test")
                    .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                    .build();
            inputStreamObserver.onNext(inputDatum);
        }

        inputStreamObserver.onCompleted();

        while(outputStreamObserver.t == null);
        assertNotNull(outputStreamObserver.t);
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }
}
