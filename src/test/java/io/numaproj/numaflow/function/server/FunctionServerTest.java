package io.numaproj.numaflow.function.server;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.common.GRPCServerConfig;
import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.MessageT;
import io.numaproj.numaflow.function.ReduceOutputStreamObserver;
import io.numaproj.numaflow.function.ReduceTestFactory;
import io.numaproj.numaflow.function.map.MapFunc;
import io.numaproj.numaflow.function.mapt.MapTFunc;
import io.numaproj.numaflow.function.v1.Udfunction;
import io.numaproj.numaflow.function.v1.Udfunction.EventTime;
import io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;
import java.util.function.BiFunction;

import static io.numaproj.numaflow.function.FunctionConstants.DATUM_KEY;
import static io.numaproj.numaflow.function.FunctionConstants.WIN_END_KEY;
import static io.numaproj.numaflow.function.FunctionConstants.WIN_START_KEY;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class FunctionServerTest {
    private final static String PROCESSED_KEY_SUFFIX = "-key-processed";
    private final static String REDUCE_PROCESSED_KEY_SUFFIX = "-processed";
    private final static String PROCESSED_VALUE_SUFFIX = "-value-processed";
    private final static Instant TEST_EVENT_TIME = Instant.MIN;

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private final BiFunction<String, Datum, Message[]> testMapFn =
            (key, datum) -> new Message[]{new Message(
                    key + PROCESSED_KEY_SUFFIX,
                    (new String(datum.getValue())
                            + PROCESSED_VALUE_SUFFIX).getBytes())};

    private final BiFunction<String, Datum, MessageT[]> testMapTFn =
            (key, datum) -> new MessageT[]{new MessageT(
                    TEST_EVENT_TIME,
                    key + PROCESSED_KEY_SUFFIX,
                    (new String(datum.getValue())
                            + PROCESSED_VALUE_SUFFIX).getBytes())};

    private FunctionServer server;
    private ManagedChannel inProcessChannel;

    @Before
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();

        server = new FunctionServer(
                InProcessServerBuilder.forName(serverName).directExecutor(),
                new GRPCServerConfig());

        server.registerMapper(new MapFunc(testMapFn))
                .registerMapperT(new MapTFunc(testMapTFn))
                .registerReducerFactory(new ReduceTestFactory())
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

        String expectedKey = "inkey" + PROCESSED_KEY_SUFFIX;
        ByteString expectedValue = ByteString.copyFromUtf8("invalue" + PROCESSED_VALUE_SUFFIX);

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
    public void mapperT() {
        ByteString inValue = ByteString.copyFromUtf8("invalue");
        Udfunction.Datum inDatum = Udfunction.Datum
                .newBuilder()
                .setKey("not-my-key")
                .setValue(inValue)
                .build();

        String expectedKey = "inkey" + PROCESSED_KEY_SUFFIX;
        ByteString expectedValue = ByteString.copyFromUtf8("invalue" + PROCESSED_VALUE_SUFFIX);

        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of(DATUM_KEY, Metadata.ASCII_STRING_MARSHALLER), "inkey");

        var stub = UserDefinedFunctionGrpc.newBlockingStub(inProcessChannel);
        var actualDatumList = stub
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                .mapTFn(inDatum);

        assertEquals(1, actualDatumList.getElementsCount());
        assertEquals(
                EventTime.newBuilder().setEventTime(
                        com.google.protobuf.Timestamp.newBuilder()
                                .setSeconds(TEST_EVENT_TIME.getEpochSecond())
                                .setNanos(TEST_EVENT_TIME.getNano())).build(),
                actualDatumList.getElements(0).getEventTime());
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

        String expectedKey = reduceKey + REDUCE_PROCESSED_KEY_SUFFIX;
        // sum of first 10 numbers 1 to 10 -> 55
        ByteString expectedValue = ByteString.copyFromUtf8(String.valueOf(55));
        while (!outputStreamObserver.completed.get()) ;

        assertEquals(1, outputStreamObserver.resultDatum.get().getElementsCount());
        assertEquals(expectedKey, outputStreamObserver.resultDatum.get().getElements(0).getKey());
        ;
        assertEquals(
                expectedValue,
                outputStreamObserver.resultDatum.get().getElements(0).getValue());

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

        // sum of first 10 numbers 1 to 10 -> 55
        ByteString expectedValue = ByteString.copyFromUtf8(String.valueOf(55));

        while (!outputStreamObserver.completed.get()) ;
        Udfunction.DatumList result = outputStreamObserver.resultDatum.get();
        assertEquals(100, result.getElementsCount());
        for (int i = 0; i < keyCount; i++) {
            assertEquals(expectedValue, result.getElements(0).getValue());
        }
    }
}
