package io.numaproj.numaflow.sessionreducer;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
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
import java.util.concurrent.atomic.AtomicInteger;

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
    public void open_append_close() {
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
                Sessionreduce.SessionReduceRequest request;
                if (i == 1) {
                    request = Sessionreduce.SessionReduceRequest
                            .newBuilder()
                            .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                    .newBuilder()
                                    .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.OPEN_VALUE)
                                    .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow
                                            .newBuilder()
                                            .addAllKeys(List.of(reduceKey + j))
                                            .setStart(Timestamp
                                                    .newBuilder().setSeconds(6000).build())
                                            .setEnd(Timestamp.newBuilder().setSeconds(7000).build())
                                            .setSlot("test-slot")
                                            .build()))
                                    .build())
                            .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                    .addAllKeys(List.of(reduceKey + j))
                                    .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                                    .build())
                            .build();
                } else {
                    request = Sessionreduce.SessionReduceRequest
                            .newBuilder()
                            .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                    .newBuilder()
                                    .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.APPEND_VALUE)
                                    .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow
                                            .newBuilder()
                                            .addAllKeys(List.of(reduceKey + j))
                                            .setStart(Timestamp
                                                    .newBuilder().setSeconds(6000).build())
                                            .setEnd(Timestamp.newBuilder().setSeconds(7000).build())
                                            .setSlot("test-slot")
                                            .build()))
                                    .build())
                            .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                    .addAllKeys(List.of(reduceKey + j))
                                    .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                                    .build())
                            .build();
                }
                inputStreamObserver.onNext(request);
            }
        }
        // close the keyed windows.
        for (int i = 0; i < keyCount; i++) {
            Sessionreduce.SessionReduceRequest closeRequest = Sessionreduce.SessionReduceRequest
                    .newBuilder()
                    .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                            .newBuilder()
                            .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.CLOSE_VALUE)
                            .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                    .addAllKeys(List.of(reduceKey + i))
                                    .setSlot("test-slot")
                                    .setStart(Timestamp
                                            .newBuilder().setSeconds(6000).build())
                                    .setEnd(Timestamp.newBuilder().setSeconds(7000).build())
                                    .build()))
                            .build())
                    .build();
            inputStreamObserver.onNext(closeRequest);
        }

        // sum of first 10 numbers 1 to 10 -> 55
        ByteString expectedFirstResponse = ByteString.copyFromUtf8(String.valueOf(55));
        // after the sum reaches 55, the test reducer reset the sum, hence when EOF is sent from input stream, the sum is 11 and gets sent to output stream.
        ByteString expectedSecondResponse = ByteString.copyFromUtf8(String.valueOf(11));

        while (!outputStreamObserver.completed.get()) ;
        List<Sessionreduce.SessionReduceResponse> result = outputStreamObserver.resultDatum.get();
        // the outputStreamObserver should have observed 3*keyCount responses, 2 with real output sum data, one as EOF.
        assertEquals(keyCount * 3, result.size());
        result.forEach(response -> assertTrue(
                response.getKeyedWindow().getStart().equals(Timestamp
                        .newBuilder()
                        .setSeconds(6000)
                        .build()) && response.getKeyedWindow().getEnd().equals(Timestamp
                        .newBuilder()
                        .setSeconds(7000)
                        .build()) &&
                        (response.getResult().getValue().equals(expectedFirstResponse) ||
                                response.getResult().getValue().equals(expectedSecondResponse)
                                || response.getEOF())));
    }

    @Test
    public void open_expand_close() {
        // create an output stream observer
        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();
        StreamObserver<Sessionreduce.SessionReduceRequest> inputStreamObserver = SessionReduceGrpc
                .newStub(inProcessChannel)
                .sessionReduceFn(outputStreamObserver);

        List<Sessionreduce.SessionReduceRequest> requests = List.of(
                // open a window for key client1, value 10. window start 60000, end 70000
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.OPEN_VALUE)
                                .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("client1"))
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(60000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(70000).build())
                                        .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client1"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(10)))
                                .build())
                        .build(),
                // open a window for key client2, value 20. window start 60000, end 70000
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.OPEN_VALUE)
                                .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("client2"))
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(60000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(70000).build())
                                        .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client2"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(20)))
                                .build())
                        .build(),
                // expand the window for key client1, value 10. expand [60000, 70000] to [60000, 75000]
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.EXPAND_VALUE)
                                .addAllKeyedWindows(List.of(
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client1"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(60000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(70000)
                                                        .build())
                                                .setSlot("slot-0").build(),
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client1"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(60000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(75000)
                                                        .build())
                                                .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client1"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(10)))
                                .build())
                        .build(),
                // expand the window for key client2, value 20. expand [60000, 70000] to [60000, 79000]
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.EXPAND_VALUE)
                                .addAllKeyedWindows(List.of(
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client2"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(60000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(70000)
                                                        .build())
                                                .setSlot("slot-0").build(),
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client2"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(60000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(79000)
                                                        .build())
                                                .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client2"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(20)))
                                .build())
                        .build(),
                // close both expanded windows
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.CLOSE_VALUE)
                                .addAllKeyedWindows(List.of(
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client1"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(60000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(75000)
                                                        .build())
                                                .setSlot("slot-0").build(),
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client2"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(60000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(79000)
                                                        .build())
                                                .setSlot("slot-0").build()
                                ))
                                .build())
                        .build()
        );

        // send the test requests one by one to the input stream.
        for (Sessionreduce.SessionReduceRequest request : requests) {
            inputStreamObserver.onNext(request);
        }

        while (!outputStreamObserver.completed.get()) ;
        List<Sessionreduce.SessionReduceResponse> result = outputStreamObserver.resultDatum.get();
        // TODO - remove
        System.out.println("keran-merge-test: output");
        result.forEach(sessionReduceResponse -> System.out.println(sessionReduceResponse));

        assertEquals(4, result.size());
        assertEquals(
                Sessionreduce.SessionReduceResponse.newBuilder()
                        .setEOF(false)
                        .setKeyedWindow(Sessionreduce.KeyedWindow.newBuilder()
                                .setStart(Timestamp.newBuilder().setSeconds(60000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(75000).build())
                                .addAllKeys(List.of("client1"))
                                .setSlot("slot-0")
                                .build())
                        .setResult(Sessionreduce.SessionReduceResponse.Result.newBuilder()
                                .addAllKeys(List.of("client1" + REDUCE_PROCESSED_KEY_SUFFIX))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(20))))
                        .build(),
                result.get(0));
        assertEquals(
                Sessionreduce.SessionReduceResponse.newBuilder()
                        .setEOF(false)
                        .setKeyedWindow(Sessionreduce.KeyedWindow.newBuilder()
                                .setStart(Timestamp.newBuilder().setSeconds(60000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(79000).build())
                                .addAllKeys(List.of("client2"))
                                .setSlot("slot-0")
                                .build())
                        .setResult(Sessionreduce.SessionReduceResponse.Result.newBuilder()
                                .addAllKeys(List.of("client2" + REDUCE_PROCESSED_KEY_SUFFIX))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(40))))
                        .build(),
                result.get(1));
        assertEquals(
                Sessionreduce.SessionReduceResponse.newBuilder()
                        .setEOF(true)
                        .setKeyedWindow(Sessionreduce.KeyedWindow.newBuilder()
                                .setStart(Timestamp.newBuilder().setSeconds(60000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(75000).build())
                                .addAllKeys(List.of("client1"))
                                .setSlot("slot-0")
                                .build())
                        .build(),
                result.get(2));
        assertEquals(
                Sessionreduce.SessionReduceResponse.newBuilder()
                        .setEOF(true)
                        .setKeyedWindow(Sessionreduce.KeyedWindow.newBuilder()
                                .setStart(Timestamp.newBuilder().setSeconds(60000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(79000).build())
                                .addAllKeys(List.of("client2"))
                                .setSlot("slot-0")
                                .build())
                        .build(),
                result.get(3));
    }

    @Test
    public void open_merge_close() throws InterruptedException {
        // create an output stream observer
        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();
        StreamObserver<Sessionreduce.SessionReduceRequest> inputStreamObserver = SessionReduceGrpc
                .newStub(inProcessChannel)
                .sessionReduceFn(outputStreamObserver);

        List<Sessionreduce.SessionReduceRequest> requests = List.of(
                // open a window for key client1, value 10. window start 60000, end 70000
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.OPEN_VALUE)
                                .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("client1"))
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(60000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(70000).build())
                                        .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client1"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(10)))
                                .build())
                        .build(),
                // open a window for key client2, value 20. window start 60000, end 70000
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.OPEN_VALUE)
                                .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("client2"))
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(60000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(70000).build())
                                        .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client2"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(20)))
                                .build())
                        .build(),
                // open a window for key client1, value 10. window start 75000, end 85000
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.OPEN_VALUE)
                                .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("client1"))
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(75000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(85000).build())
                                        .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client1"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(10)))
                                .build())
                        .build(),
                // open a window for key client2, value 20. window start 78000, end 88000
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.OPEN_VALUE)
                                .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("client2"))
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(78000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(88000).build())
                                        .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client2"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(20)))
                                .build())
                        .build(),
                // merge two windows for key client1
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.MERGE_VALUE)
                                .addAllKeyedWindows(List.of(
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client1"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(60000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(70000)
                                                        .build())
                                                .setSlot("slot-0").build(),
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client1"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(75000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(85000)
                                                        .build())
                                                .setSlot("slot-0").build()
                                ))
                                .build())
                        .build(),
                // merge two windows for key client2
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.MERGE_VALUE)
                                .addAllKeyedWindows(List.of(
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client2"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(60000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(70000)
                                                        .build())
                                                .setSlot("slot-0").build(),
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client2"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(78000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(88000)
                                                        .build())
                                                .setSlot("slot-0").build()
                                ))
                                .build())
                        .build(),
                // close both merged windows
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.CLOSE_VALUE)
                                .addAllKeyedWindows(List.of(
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client1"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(60000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(85000)
                                                        .build())
                                                .setSlot("slot-0").build(),
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client2"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(60000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(88000)
                                                        .build())
                                                .setSlot("slot-0").build()
                                ))
                                .build())
                        .build()
        );

        // send the test requests one by one to the input stream.
        for (Sessionreduce.SessionReduceRequest request : requests) {
            // temporarily add a sleep to make sure the requests are sent one by one
            // TODO - to fix this, we need to ensure close request gets properly processed even when the window is in the process of merging.
            Thread.sleep(1000);
            inputStreamObserver.onNext(request);
        }

        while (!outputStreamObserver.completed.get()) ;
        List<Sessionreduce.SessionReduceResponse> result = outputStreamObserver.resultDatum.get();
        // TODO - remove
        System.out.println("keran-merge-test: output");
        result.forEach(sessionReduceResponse -> System.out.println(sessionReduceResponse));

        assertEquals(4, result.size());
        assertEquals(
                Sessionreduce.SessionReduceResponse.newBuilder()
                        .setEOF(false)
                        .setKeyedWindow(Sessionreduce.KeyedWindow.newBuilder()
                                .setStart(Timestamp.newBuilder().setSeconds(60000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(85000).build())
                                .addAllKeys(List.of("client1"))
                                .setSlot("slot-0")
                                .build())
                        .setResult(Sessionreduce.SessionReduceResponse.Result.newBuilder()
                                .addAllKeys(List.of("client1" + REDUCE_PROCESSED_KEY_SUFFIX))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(20))))
                        .build(),
                result.get(0));
        assertEquals(
                Sessionreduce.SessionReduceResponse.newBuilder()
                        .setEOF(false)
                        .setKeyedWindow(Sessionreduce.KeyedWindow.newBuilder()
                                .setStart(Timestamp.newBuilder().setSeconds(60000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(88000).build())
                                .addAllKeys(List.of("client2"))
                                .setSlot("slot-0")
                                .build())
                        .setResult(Sessionreduce.SessionReduceResponse.Result.newBuilder()
                                .addAllKeys(List.of("client2" + REDUCE_PROCESSED_KEY_SUFFIX))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(40))))
                        .build(),
                result.get(1));
        assertEquals(
                Sessionreduce.SessionReduceResponse.newBuilder()
                        .setEOF(true)
                        .setKeyedWindow(Sessionreduce.KeyedWindow.newBuilder()
                                .setStart(Timestamp.newBuilder().setSeconds(60000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(85000).build())
                                .addAllKeys(List.of("client1"))
                                .setSlot("slot-0")
                                .build())
                        .build(),
                result.get(2));
        assertEquals(
                Sessionreduce.SessionReduceResponse.newBuilder()
                        .setEOF(true)
                        .setKeyedWindow(Sessionreduce.KeyedWindow.newBuilder()
                                .setStart(Timestamp.newBuilder().setSeconds(60000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(88000).build())
                                .addAllKeys(List.of("client2"))
                                .setSlot("slot-0")
                                .build())
                        .build(),
                result.get(3));
    }

    @Test
    public void open_expand_append_merge_close() throws InterruptedException {
        // create an output stream observer
        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();
        StreamObserver<Sessionreduce.SessionReduceRequest> inputStreamObserver = SessionReduceGrpc
                .newStub(inProcessChannel)
                .sessionReduceFn(outputStreamObserver);

        List<Sessionreduce.SessionReduceRequest> requests = List.of(
                // open a window for key client1, value 5. window start 60000, end 70000
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.OPEN_VALUE)
                                .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("client1"))
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(60000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(70000).build())
                                        .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client1"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(5)))
                                .build())
                        .build(),
                // open a window for key client2, value 10. window start 60000, end 70000
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.OPEN_VALUE)
                                .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("client2"))
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(60000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(70000).build())
                                        .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client2"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(10)))
                                .build())
                        .build(),
                // open a window for key client1, value 5. window start 75000, end 85000
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.OPEN_VALUE)
                                .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("client1"))
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(75000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(85000).build())
                                        .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client1"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(5)))
                                .build())
                        .build(),
                // open a window for key client2, value 10. window start 78000, end 88000
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.OPEN_VALUE)
                                .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("client2"))
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(78000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(88000).build())
                                        .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client2"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(10)))
                                .build())
                        .build(),
                // expand a window for key client1, value 5. expand from [75000, 85000] to [75000, 95000]
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.EXPAND_VALUE)
                                .addAllKeyedWindows(
                                        List.of(
                                                Sessionreduce.KeyedWindow.newBuilder()
                                                        .addAllKeys(List.of("client1"))
                                                        .setStart(Timestamp
                                                                .newBuilder()
                                                                .setSeconds(75000)
                                                                .build())
                                                        .setEnd(Timestamp
                                                                .newBuilder()
                                                                .setSeconds(85000)
                                                                .build())
                                                        .setSlot("slot-0").build(),
                                                Sessionreduce.KeyedWindow.newBuilder()
                                                        .addAllKeys(List.of("client1"))
                                                        .setStart(Timestamp
                                                                .newBuilder()
                                                                .setSeconds(75000)
                                                                .build())
                                                        .setEnd(Timestamp
                                                                .newBuilder()
                                                                .setSeconds(95000)
                                                                .build())
                                                        .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client1"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(5)))
                                .build())
                        .build(),
                // expand a window for key client2, value 10. expand from [78000, 88000] to [78000, 98000]
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.EXPAND_VALUE)
                                .addAllKeyedWindows(
                                        List.of(
                                                Sessionreduce.KeyedWindow.newBuilder()
                                                        .addAllKeys(List.of("client2"))
                                                        .setStart(Timestamp
                                                                .newBuilder()
                                                                .setSeconds(78000)
                                                                .build())
                                                        .setEnd(Timestamp
                                                                .newBuilder()
                                                                .setSeconds(88000)
                                                                .build())
                                                        .setSlot("slot-0").build(),
                                                Sessionreduce.KeyedWindow.newBuilder()
                                                        .addAllKeys(List.of("client2"))
                                                        .setStart(Timestamp
                                                                .newBuilder()
                                                                .setSeconds(78000)
                                                                .build())
                                                        .setEnd(Timestamp
                                                                .newBuilder()
                                                                .setSeconds(98000)
                                                                .build())
                                                        .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client2"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(10)))
                                .build())
                        .build(),
                // append to the key client1 with value 5
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.APPEND_VALUE)
                                .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow
                                        .newBuilder()
                                        .addAllKeys(List.of("client1"))
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(75000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(95000).build())
                                        .setSlot("slot-0")
                                        .build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client1"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(5)))
                                .build())
                        .build(),
                // append to the key client2 with value 10
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.APPEND_VALUE)
                                .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow
                                        .newBuilder()
                                        .addAllKeys(List.of("client2"))
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(78000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(98000).build())
                                        .setSlot("slot-0")
                                        .build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client2"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(10)))
                                .build())
                        .build(),
                // merge two windows for key client1
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.MERGE_VALUE)
                                .addAllKeyedWindows(List.of(
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client1"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(60000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(70000)
                                                        .build())
                                                .setSlot("slot-0").build(),
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client1"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(75000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(95000)
                                                        .build())
                                                .setSlot("slot-0").build()
                                ))
                                .build())
                        .build(),
                // merge two windows for key client2
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.MERGE_VALUE)
                                .addAllKeyedWindows(List.of(
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client2"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(60000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(70000)
                                                        .build())
                                                .setSlot("slot-0").build(),
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client2"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(78000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(98000)
                                                        .build())
                                                .setSlot("slot-0").build()
                                ))
                                .build())
                        .build(),
                // close both merged windows
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.CLOSE_VALUE)
                                .addAllKeyedWindows(List.of(
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client1"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(60000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(95000)
                                                        .build())
                                                .setSlot("slot-0").build(),
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client2"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(60000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(98000)
                                                        .build())
                                                .setSlot("slot-0").build()
                                ))
                                .build())
                        .build()
        );

        // send the test requests one by one to the input stream.
        for (Sessionreduce.SessionReduceRequest request : requests) {
            // temporarily add a sleep to make sure the requests are sent one by one
            // TODO - to fix this, we need to ensure close request gets properly processed even when the window is in the process of merging.
            Thread.sleep(1000);
            inputStreamObserver.onNext(request);
        }

        while (!outputStreamObserver.completed.get()) ;
        List<Sessionreduce.SessionReduceResponse> result = outputStreamObserver.resultDatum.get();
        // TODO - remove
        System.out.println("keran-merge-test: output");
        result.forEach(sessionReduceResponse -> System.out.println(sessionReduceResponse));

        assertEquals(4, result.size());
        assertEquals(
                Sessionreduce.SessionReduceResponse.newBuilder()
                        .setEOF(false)
                        .setKeyedWindow(Sessionreduce.KeyedWindow.newBuilder()
                                .setStart(Timestamp.newBuilder().setSeconds(60000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(95000).build())
                                .addAllKeys(List.of("client1"))
                                .setSlot("slot-0")
                                .build())
                        .setResult(Sessionreduce.SessionReduceResponse.Result.newBuilder()
                                .addAllKeys(List.of("client1" + REDUCE_PROCESSED_KEY_SUFFIX))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(20))))
                        .build(),
                result.get(0));
        assertEquals(
                Sessionreduce.SessionReduceResponse.newBuilder()
                        .setEOF(false)
                        .setKeyedWindow(Sessionreduce.KeyedWindow.newBuilder()
                                .setStart(Timestamp.newBuilder().setSeconds(60000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(98000).build())
                                .addAllKeys(List.of("client2"))
                                .setSlot("slot-0")
                                .build())
                        .setResult(Sessionreduce.SessionReduceResponse.Result.newBuilder()
                                .addAllKeys(List.of("client2" + REDUCE_PROCESSED_KEY_SUFFIX))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(40))))
                        .build(),
                result.get(1));
        assertEquals(
                Sessionreduce.SessionReduceResponse.newBuilder()
                        .setEOF(true)
                        .setKeyedWindow(Sessionreduce.KeyedWindow.newBuilder()
                                .setStart(Timestamp.newBuilder().setSeconds(60000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(95000).build())
                                .addAllKeys(List.of("client1"))
                                .setSlot("slot-0")
                                .build())
                        .build(),
                result.get(2));
        assertEquals(
                Sessionreduce.SessionReduceResponse.newBuilder()
                        .setEOF(true)
                        .setKeyedWindow(Sessionreduce.KeyedWindow.newBuilder()
                                .setStart(Timestamp.newBuilder().setSeconds(60000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(98000).build())
                                .addAllKeys(List.of("client2"))
                                .setSlot("slot-0")
                                .build())
                        .build(),
                result.get(3));
    }

    public static class SessionReducerTestFactory extends SessionReducerFactory<SessionReducerTestFactory.TestSessionReducerHandler> {
        @Override
        public TestSessionReducerHandler createSessionReducer() {
            return new TestSessionReducerHandler();
        }

        public static class TestSessionReducerHandler extends SessionReducer {
            private AtomicInteger sum = new AtomicInteger(0);

            @Override
            public void processMessage(
                    String[] keys,
                    Datum datum,
                    OutputStreamObserver outputStreamObserver) {
                sum.addAndGet(Integer.parseInt(new String(datum.getValue())));
                if (sum.get() > 50) {
                    String[] updatedKeys = Arrays
                            .stream(keys)
                            .map(c -> c + REDUCE_PROCESSED_KEY_SUFFIX)
                            .toArray(String[]::new);
                    Message message = new Message(
                            String.valueOf(sum.get()).getBytes(),
                            updatedKeys);
                    outputStreamObserver.send(message);
                    // reset sum
                    sum.set(0);
                }
            }

            @Override
            public void handleEndOfStream(
                    String[] keys,
                    OutputStreamObserver outputStreamObserver) {
                String[] updatedKeys = Arrays
                        .stream(keys)
                        .map(c -> c + REDUCE_PROCESSED_KEY_SUFFIX)
                        .toArray(String[]::new);
                Message message = new Message(String.valueOf(sum.get()).getBytes(), updatedKeys);
                outputStreamObserver.send(message);
            }

            @Override
            public byte[] accumulator() {
                return String.valueOf(sum.get()).getBytes();
            }

            @Override
            public void mergeAccumulator(byte[] accumulator) {
                Integer value = Integer.parseInt(new String(accumulator));
                sum.addAndGet(value);
            }
        }
    }
}
