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

// TODO - add a couple of more tests.
// 1. early return
// 3. receive EOF of gRPC stream, automatically close all windows.
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
        // create an output stream observer
        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();
        StreamObserver<Sessionreduce.SessionReduceRequest> inputStreamObserver = SessionReduceGrpc
                .newStub(inProcessChannel)
                .sessionReduceFn(outputStreamObserver);

        List<Sessionreduce.SessionReduceRequest> requests = List.of(
                // open a window for key client1, value 5. window start 60000, end 120000
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.OPEN_VALUE)
                                .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("client1"))
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(60000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(120000).build())
                                        .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client1"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(5)))
                                .build())
                        .build(),
                // append to key client, value 10.
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.APPEND_VALUE)
                                .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("client1"))
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(60000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(120000).build())
                                        .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client1"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(10)))
                                .build())
                        .build(),
                // append to key client, value 15.
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.APPEND_VALUE)
                                .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("client1"))
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(60000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(120000).build())
                                        .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client1"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(15)))
                                .build())
                        .build(),
                // append to key client without any value.
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.APPEND_VALUE)
                                .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("client1"))
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(60000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(120000).build())
                                        .setSlot("slot-0").build()))
                                .build())
                        .build(),
                // close the window
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
                                                        .setSeconds(120000)
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
        assertEquals(2, result.size());
        assertEquals(
                Sessionreduce.SessionReduceResponse.newBuilder()
                        .setEOF(false)
                        .setKeyedWindow(Sessionreduce.KeyedWindow.newBuilder()
                                .setStart(Timestamp.newBuilder().setSeconds(60000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(120000).build())
                                .addAllKeys(List.of("client1"))
                                .setSlot("slot-0")
                                .build())
                        .setResult(Sessionreduce.SessionReduceResponse.Result.newBuilder()
                                .addAllKeys(List.of("client1" + REDUCE_PROCESSED_KEY_SUFFIX))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(30))))
                        .build(),
                result.get(0));
        assertEquals(
                Sessionreduce.SessionReduceResponse.newBuilder()
                        .setEOF(true)
                        .setKeyedWindow(Sessionreduce.KeyedWindow.newBuilder()
                                .setStart(Timestamp.newBuilder().setSeconds(60000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(120000).build())
                                .addAllKeys(List.of("client1"))
                                .setSlot("slot-0")
                                .build())
                        .build(),
                result.get(1));
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
    public void open_merge_close() {
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
            inputStreamObserver.onNext(request);
        }

        while (!outputStreamObserver.completed.get()) ;
        List<Sessionreduce.SessionReduceResponse> result = outputStreamObserver.resultDatum.get();

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
    public void open_expand_append_merge_close() {
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
            inputStreamObserver.onNext(request);
        }

        while (!outputStreamObserver.completed.get()) ;
        List<Sessionreduce.SessionReduceResponse> result = outputStreamObserver.resultDatum.get();

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

    // till now, we have completed the Java version of the unit tests in Go SDK package: https://github.com/numaproj/numaflow-go/blob/main/pkg/sessionreducer/service_test.go
    // below are more tests that are NOT in the numaflow-go unit tests.
    @Test
    public void open_merge_close_mergeIntoAnExistingWindow() {
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
                // open a window for key client1, value 20. window start 61000, end 69000
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.OPEN_VALUE)
                                .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("client1"))
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(61000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(69000).build())
                                        .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client1"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(20)))
                                .build())
                        .build(),
                // open a window for key client1, value 1. window start 62000, end 69500
                Sessionreduce.SessionReduceRequest
                        .newBuilder()
                        .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                                .newBuilder()
                                .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.OPEN_VALUE)
                                .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("client1"))
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(62000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(69500).build())
                                        .setSlot("slot-0").build()))
                                .build())
                        .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                                .addAllKeys(List.of("client1"))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(1)))
                                .build())
                        .build(),
                // merge the windows for key client1
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
                                                        .newBuilder().setSeconds(61000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(69000)
                                                        .build())
                                                .setSlot("slot-0").build(),
                                        Sessionreduce.KeyedWindow.newBuilder()
                                                .addAllKeys(List.of("client1"))
                                                .setStart(Timestamp
                                                        .newBuilder().setSeconds(62000).build())
                                                .setEnd(Timestamp
                                                        .newBuilder()
                                                        .setSeconds(69500)
                                                        .build())
                                                .setSlot("slot-0").build()
                                ))
                                .build())
                        .build(),
                // close the merged window
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
                                                        .setSeconds(70000)
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

        assertEquals(2, result.size());
        assertEquals(
                Sessionreduce.SessionReduceResponse.newBuilder()
                        .setEOF(false)
                        .setKeyedWindow(Sessionreduce.KeyedWindow.newBuilder()
                                .setStart(Timestamp.newBuilder().setSeconds(60000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(70000).build())
                                .addAllKeys(List.of("client1"))
                                .setSlot("slot-0")
                                .build())
                        .setResult(Sessionreduce.SessionReduceResponse.Result.newBuilder()
                                .addAllKeys(List.of("client1" + REDUCE_PROCESSED_KEY_SUFFIX))
                                .setValue(ByteString.copyFromUtf8(String.valueOf(31))))
                        .build(),
                result.get(0));
        assertEquals(
                Sessionreduce.SessionReduceResponse.newBuilder()
                        .setEOF(true)
                        .setKeyedWindow(Sessionreduce.KeyedWindow.newBuilder()
                                .setStart(Timestamp.newBuilder().setSeconds(60000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(70000).build())
                                .addAllKeys(List.of("client1"))
                                .setSlot("slot-0")
                                .build())
                        .build(),
                result.get(1));
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
