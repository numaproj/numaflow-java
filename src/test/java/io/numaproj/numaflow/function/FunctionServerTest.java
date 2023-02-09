package io.numaproj.numaflow.function;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.common.GrpcServerConfig;
import io.numaproj.numaflow.function.map.MapFunc;
import io.numaproj.numaflow.function.v1.Udfunction;
import io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.function.BiFunction;
import java.util.logging.Logger;

import static io.numaproj.numaflow.function.Function.DATUM_KEY;
import static io.numaproj.numaflow.function.Function.WIN_END_KEY;
import static io.numaproj.numaflow.function.Function.WIN_START_KEY;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class FunctionServerTest {
    private static final Logger logger = Logger.getLogger(FunctionServerTest.class.getName());
    private final static String processedKeySuffix = "-key-processed";

    private final static String reduceProcessedKeySuffix = "-processed";
    private final static String processedValueSuffix = "-value-processed";
    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private final BiFunction<String, Datum, Message[]> testMapFn =
            (key, datum) -> new Message[]{new Message(
                    key + processedKeySuffix,
                    (new String(datum.getValue())
                            + processedValueSuffix).getBytes())};

    private FunctionServer server;
    private ManagedChannel inProcessChannel;

    @Before
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        server = new FunctionServer(
                InProcessServerBuilder.forName(serverName).directExecutor(),
                new GrpcServerConfig(Function.SOCKET_PATH, Function.DEFAULT_MESSAGE_SIZE));
        server
                .registerMapper(new MapFunc(testMapFn))
                .registerReducer(ReduceTestFn.class)
                .start();
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
    public void mapper() {
        ByteString inValue = ByteString.copyFromUtf8("invalue");
        Udfunction.Datum inDatum = Udfunction.Datum
                .newBuilder()
                .setKey("not-my-key")
                .setValue(inValue)
                .build();

        String expectedKey = "inkey" + processedKeySuffix;
        ByteString expectedValue = ByteString.copyFromUtf8("invalue" + processedValueSuffix);

        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of(DATUM_KEY, Metadata.ASCII_STRING_MARSHALLER), "inkey");

        var stub = UserDefinedFunctionGrpc.newBlockingStub(inProcessChannel);
        var actualDatumList = stub
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                .mapFn(inDatum);

        assertEquals(1, actualDatumList.getElementsCount());
        assertEquals(expectedKey, actualDatumList.getElements(0).getKey());
        assertEquals(expectedValue, actualDatumList.getElements(0).getValue());
    }

    @Test
    public void reducerWithOneKey() {
        String reduceKey = "reduce-key";
        Udfunction.Datum.Builder inDatumBuilder = Udfunction.Datum.newBuilder().setKey(reduceKey);

        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of(DATUM_KEY, Metadata.ASCII_STRING_MARSHALLER), reduceKey);
        metadata.put(Metadata.Key.of(WIN_START_KEY, Metadata.ASCII_STRING_MARSHALLER), "60000");
        metadata.put(Metadata.Key.of(WIN_END_KEY, Metadata.ASCII_STRING_MARSHALLER), "120000");

        //create an output stream observer
        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();

        StreamObserver<Udfunction.Datum> inputStreamObserver = UserDefinedFunctionGrpc
                .newStub(inProcessChannel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                .reduceFn(outputStreamObserver);

        for (int i = 1; i <= 10; i++) {
            Udfunction.Datum inputDatum = inDatumBuilder
                    .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                    .build();
            inputStreamObserver.onNext(inputDatum);
        }

        inputStreamObserver.onCompleted();

        String expectedKey = reduceKey + reduceProcessedKeySuffix;
        // sum of first 10 numbers 1 to 10 -> 55
        ByteString expectedValue = ByteString.copyFromUtf8(String.valueOf(55));
        while (outputStreamObserver.resultDatum == null);

        assertEquals(1, outputStreamObserver.resultDatum.getElementsCount());
        assertEquals(expectedKey, outputStreamObserver.resultDatum.getElements(0).getKey());
        System.out.println(new String(outputStreamObserver.resultDatum.getElements(0).getValue().toByteArray()));
        assertEquals(expectedValue, outputStreamObserver.resultDatum.getElements(0).getValue());

    }

    @Test
    public void reducerWithMultipleKey() {
        String reduceKey = "reduce-key";
        int keyCount = 100;
        Udfunction.Datum.Builder inDatumBuilder = Udfunction.Datum.newBuilder().setKey(reduceKey);

        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of(DATUM_KEY, Metadata.ASCII_STRING_MARSHALLER), reduceKey);
        metadata.put(Metadata.Key.of(WIN_START_KEY, Metadata.ASCII_STRING_MARSHALLER), "60000");
        metadata.put(Metadata.Key.of(WIN_END_KEY, Metadata.ASCII_STRING_MARSHALLER), "120000");

        //create an output stream observer
        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();

        StreamObserver<Udfunction.Datum> inputStreamObserver = UserDefinedFunctionGrpc
                .newStub(inProcessChannel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                .reduceFn(outputStreamObserver);

        // send messages with 100 different keys
        for (int j = 0; j < keyCount; j++) {
            for (int i = 1; i <= 10; i++) {
                Udfunction.Datum inputDatum = inDatumBuilder
                        .setKey(reduceKey + j)
                        .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                        .build();
                inputStreamObserver.onNext(inputDatum);
            }
        }

        inputStreamObserver.onCompleted();

        String expectedKey = reduceKey + reduceProcessedKeySuffix;
        // sum of first 10 numbers 1 to 10 -> 55
        ByteString expectedValue = ByteString.copyFromUtf8(String.valueOf(55));

        while(outputStreamObserver.resultDatum == null);
        assertEquals(100, outputStreamObserver.resultDatum.getElementsCount());
        assertEquals(expectedValue, outputStreamObserver.resultDatum.getElements(0).getValue());
    }

}
