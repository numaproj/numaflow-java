package io.numaproj.numaflow.function;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.function.handlers.MapHandler;
import io.numaproj.numaflow.function.handlers.MapStreamHandler;
import io.numaproj.numaflow.function.handlers.MapTHandler;
import io.numaproj.numaflow.function.interfaces.Datum;
import io.numaproj.numaflow.function.types.MessageList;
import io.numaproj.numaflow.function.types.MessageTList;
import io.numaproj.numaflow.function.v1.Udfunction;
import io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static io.numaproj.numaflow.function.FunctionConstants.WIN_END_KEY;
import static io.numaproj.numaflow.function.FunctionConstants.WIN_START_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
public class FunctionServerTestErr {
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private FunctionServer server;
    private ManagedChannel inProcessChannel;

    @Before
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();

        FunctionGRPCConfig grpcServerConfig = new FunctionGRPCConfig(FunctionConstants.DEFAULT_MESSAGE_SIZE);
        grpcServerConfig.setInfoFilePath("/tmp/numaflow-test-server-info");
        server = new FunctionServer(
                InProcessServerBuilder.forName(serverName).directExecutor(),
                grpcServerConfig);

        server.registerMapHandler(new TestMapFnErr())
                .registerMapStreamHandler(new TestMapStreamFnErr())
                .registerMapTHandler(new TestMapTFnErr())
                .registerReducerFactory(new ReduceTestFactoryErr())
                .start();

        inProcessChannel = grpcCleanup.register(InProcessChannelBuilder
                .forName(serverName)
                .directExecutor()
                .build());
    }

    @After
    public void tearDown() {
        server.stop();
    }

    @Test
    public void mapperErr() {
        ByteString inValue = ByteString.copyFromUtf8("invalue");
        Udfunction.DatumRequest inDatum = Udfunction.DatumRequest
                .newBuilder()
                .addAllKeys(List.of("test-map-key"))
                .setValue(inValue)
                .build();

        var stub = UserDefinedFunctionGrpc.newBlockingStub(inProcessChannel);
        try {
            stub.mapFn(inDatum);
            fail("Expected the mapperErr to complete with exception");
        } catch (Exception e) {
            assertEquals("UNKNOWN: unknown exception", e.getMessage());
        }
    }

    @Test
    public void mapperStream() {
        ByteString inValue = ByteString.copyFromUtf8("invalue");
        Udfunction.DatumRequest inDatum = Udfunction.DatumRequest
                .newBuilder()
                .addAllKeys(List.of("test-map-key"))
                .setValue(inValue)
                .build();

        var stub = UserDefinedFunctionGrpc.newBlockingStub(inProcessChannel);
        try {
            stub.mapStreamFn(inDatum);
        } catch (Exception e) {
            assertEquals("UNKNOWN: unknown exception", e.getMessage());
        }
    }

    @Test
    public void mapperT() {
        ByteString inValue = ByteString.copyFromUtf8("invalue");
        Udfunction.DatumRequest inDatum = Udfunction.DatumRequest
                .newBuilder()
                .addKeys("test-map-key")
                .setValue(inValue)
                .build();

        var stub = UserDefinedFunctionGrpc.newBlockingStub(inProcessChannel);
        try {
            stub.mapTFn(inDatum);
        } catch (Exception e) {
            assertEquals("UNKNOWN: unknown exception", e.getMessage());
        }
    }

    @Test
    public void reducerWithOneKey()  {
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

        StreamObserver<Udfunction.DatumRequest> inputStreamObserver = UserDefinedFunctionGrpc
                .newStub(inProcessChannel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                .reduceFn(outputStreamObserver);

        for (int i = 1; i <= 10; i++) {
            Udfunction.DatumRequest inputDatum = Udfunction.DatumRequest.newBuilder()
                    .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                    .addKeys(reduceKey)
                    .build();
            inputStreamObserver.onNext(inputDatum);
        }

        inputStreamObserver.onCompleted();

        try {
            t.join();
        } catch (InterruptedException e) {
            fail("Thread interrupted");
        }
    }

    @Test
    public void reducerWithMultipleKey() {
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

        StreamObserver<Udfunction.DatumRequest> inputStreamObserver = UserDefinedFunctionGrpc
                .newStub(inProcessChannel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                .reduceFn(outputStreamObserver);
        for (int i = 1; i <= 10; i++) {
            Udfunction.DatumRequest inputDatum = Udfunction.DatumRequest.newBuilder()
                    .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                    .addKeys(reduceKey)
                    .build();
            inputStreamObserver.onNext(inputDatum);
        }

        inputStreamObserver.onCompleted();

        try {
            t.join();
        } catch (InterruptedException e) {
            fail("Thread interrupted");
        }
    }

    private static class TestMapFnErr extends MapHandler {
        @Override
        public MessageList processMessage(String[] keys, Datum datum) {
            throw new RuntimeException("unknown exception");
        }
    }

    private static class TestMapStreamFnErr extends MapStreamHandler {
        @Override
        public void processMessage(String[] keys, Datum datum, StreamObserver<Udfunction.DatumResponse> streamObserver) {
            throw new RuntimeException("unknown exception");
        }
    }

    private static class TestMapTFnErr extends MapTHandler {
        @Override
        public MessageTList processMessage(String[] keys, Datum datum) {
            throw new RuntimeException("unknown exception");
        }
    }
}
