package io.numaproj.numaflow.batchmapper;

import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCallListener;
import io.grpc.ManagedChannel;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.batchmap.v1.BatchMapGrpc;
import io.numaproj.numaflow.batchmap.v1.Batchmap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ServerErrTest {

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
                    io.grpc.Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {

                final var context =
                        Context.current();
                ServerCall.Listener<ReqT> listener = Contexts.interceptCall(
                        context,
                        call,
                        headers,
                        next);
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(
                        listener) {
                    @Override
                    public void onHalfClose() {
                        try {
                            super.onHalfClose();
                        } catch (RuntimeException ex) {
                            handleException(ex, call, headers);
                            throw ex;
                        }
                    }

                    private void handleException(
                            RuntimeException e,
                            ServerCall<ReqT, RespT> serverCall,
                            io.grpc.Metadata headers) {
                        // Currently, we only have application level exceptions.
                        // Translate it to UNKNOWN status.
                        var status = Status.UNKNOWN.withDescription(e.getMessage()).withCause(e);
                        var newStatus = Status.fromThrowable(status.asException());
                        serverCall.close(newStatus, headers);
                    }
                };
            }
        };

        String serverName = InProcessServerBuilder.generateName();

        GRPCConfig grpcServerConfig = GRPCConfig.newBuilder()
                .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
                .socketPath(Constants.DEFAULT_SOCKET_PATH)
                .infoFilePath("/tmp/numaflow-test-server-info)")
                .build();

        server = new Server(
                new TestMapFn(),
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
    public void testErrorFromUDF() {

        BatchMapOutputStreamObserver outputStreamObserver = new BatchMapOutputStreamObserver();
        StreamObserver<Batchmap.BatchMapRequest> inputStreamObserver = BatchMapGrpc
                .newStub(inProcessChannel)
                .batchMapFn(outputStreamObserver);

        // we need to maintain a reference to any exceptions thrown inside the thread, otherwise even if the assertion failed in the thread,
        // the test can still succeed.
        AtomicReference<Throwable> exceptionInThread = new AtomicReference<>();

        Thread t = new Thread(() -> {
            while (outputStreamObserver.t == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    exceptionInThread.set(e);
                }
            }
            try {
                assertEquals(
                        "UNKNOWN: java.lang.RuntimeException: unknown exception",
                        outputStreamObserver.t.getMessage());
            } catch (Throwable e) {
                exceptionInThread.set(e);
            }
        });
        t.start();
        String message = "message";
        Batchmap.BatchMapRequest request = Batchmap.BatchMapRequest.newBuilder()
                .setValue(ByteString.copyFromUtf8(message))
                .addKeys("exception")
                .setId("exception")
                .build();
        inputStreamObserver.onNext(request);
        inputStreamObserver.onCompleted();

        try {
            t.join();
        } catch (InterruptedException e) {
            fail("Thread got interrupted before test assertion.");
        }
        // Fail the test if any exception caught in the thread
        if (exceptionInThread.get() != null) {
            fail("Assertion failed in the thread: " + exceptionInThread.get().getMessage());
        }
    }

    @Test
    public void testMismatchSizeError() {

        BatchMapOutputStreamObserver outputStreamObserver = new BatchMapOutputStreamObserver();
        StreamObserver<Batchmap.BatchMapRequest> inputStreamObserver = BatchMapGrpc
                .newStub(inProcessChannel)
                .batchMapFn(outputStreamObserver);

        // we need to maintain a reference to any exceptions thrown inside the thread, otherwise even if the assertion failed in the thread,
        // the test can still succeed.
        AtomicReference<Throwable> exceptionInThread = new AtomicReference<>();

        Thread t = new Thread(() -> {
            while (outputStreamObserver.t == null) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    exceptionInThread.set(e);
                }
            }
            try {
                assertEquals(
                        "UNKNOWN: Number of results did not match expected 2 but got 1",
                        outputStreamObserver.t.getMessage());
            } catch (Throwable e) {
                exceptionInThread.set(e);
            }
        });
        t.start();
        String message = "message";
        Batchmap.BatchMapRequest request = Batchmap.BatchMapRequest.newBuilder()
                .setValue(ByteString.copyFromUtf8(message))
                .addKeys("drop")
                .setId("drop")
                .build();
        inputStreamObserver.onNext(request);
        Batchmap.BatchMapRequest request1 = Batchmap.BatchMapRequest.newBuilder()
                .setValue(ByteString.copyFromUtf8(message))
                .addKeys("test")
                .setId("test")
                .build();
        inputStreamObserver.onNext(request1);
        inputStreamObserver.onCompleted();

        try {
            t.join();
        } catch (InterruptedException e) {
            fail("Thread got interrupted before test assertion.");
        }
        // Fail the test if any exception caught in the thread
        if (exceptionInThread.get() != null) {
            fail("Assertion failed in the thread: " + exceptionInThread.get().getMessage());
        }
    }

    private static class TestMapFn extends BatchMapper {

        @Override
        public BatchResponses processMessage(DatumIterator datumStream) {
            BatchResponses batchResponses = new BatchResponses();
            while (true) {
                Datum datum = null;
                try {
                    datum = datumStream.next();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    continue;
                }
                if (datum == null) {
                    break;
                }
                if (datum.getId().equals("exception")) {
                    throw new RuntimeException("unknown exception");
                } else if (!datum.getId().equals("drop")) {
                String msg = new String(datum.getValue());
                String[] strs = msg.split(",");
                BatchResponse batchResponse = new BatchResponse(datum.getId());
                for (String str : strs) {
                    batchResponse.append(new Message(str.getBytes()));
                }
                batchResponses.append(batchResponse);
                }
            }
            return batchResponses;
        }
    }
}
