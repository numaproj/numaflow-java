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
import io.numaproj.numaflow.map.v1.MapGrpc;
import io.numaproj.numaflow.map.v1.MapOuterClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

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

        BatchMapOutputStreamObserver outputStreamObserver = new BatchMapOutputStreamObserver(2);
        StreamObserver<MapOuterClass.MapRequest> inputStreamObserver = MapGrpc
                .newStub(inProcessChannel)
                .mapFn(outputStreamObserver);
        String message = "message";
        MapOuterClass.MapRequest handshakeRequest = MapOuterClass.MapRequest
                .newBuilder()
                .setHandshake(MapOuterClass.Handshake.newBuilder().setSot(true))
                .build();
        inputStreamObserver.onNext(handshakeRequest);
        MapOuterClass.MapRequest request = MapOuterClass.MapRequest.newBuilder()
                .setRequest(MapOuterClass.MapRequest.Request
                        .newBuilder()
                        .setValue(ByteString.copyFromUtf8(message))
                        .addKeys("exception"))
                .setId("exception")
                .build();
        inputStreamObserver.onNext(request);
        inputStreamObserver.onNext(MapOuterClass.MapRequest
                .newBuilder()
                .setStatus(MapOuterClass.Status.newBuilder().setEot(true))
                .build());
        inputStreamObserver.onCompleted();
        try {
            outputStreamObserver.done.get();
            fail("Expected exception not thrown");
        } catch (InterruptedException | ExecutionException e) {
            assertEquals(
                    "UNKNOWN: java.lang.RuntimeException: unknown exception",
                    e.getCause().getMessage());
        }
    }

    @Test
    public void testMapperWithoutHandshake() {
        ByteString inValue = ByteString.copyFromUtf8("invalue");
        MapOuterClass.MapRequest inDatum = MapOuterClass.MapRequest
                .newBuilder()
                .setRequest(MapOuterClass.MapRequest.Request
                        .newBuilder()
                        .setValue(inValue)
                        .addAllKeys(List.of("test-map-key"))
                        .build()).build();

        BatchMapOutputStreamObserver responseObserver = new BatchMapOutputStreamObserver(1);

        var stub = MapGrpc.newStub(inProcessChannel);
        var requestStreamObserver = stub
                .mapFn(responseObserver);

        requestStreamObserver.onNext(inDatum);

        try {
            responseObserver.done.get();
            fail("Expected an exception to be thrown");
        } catch (InterruptedException | ExecutionException e) {
            assertEquals(
                    "io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Handshake request not received",
                    e.getMessage());
        }
        requestStreamObserver.onCompleted();
    }

    private static class TestMapFn extends BatchMapper {

        @Override
        public BatchResponses processMessage(DatumIterator datumStream) {
            BatchResponses batchResponses = new BatchResponses();
            while (true) {
                Datum datum;
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
