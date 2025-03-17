package io.numaproj.numaflow.servingstore;

import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.google.rpc.Code;
import com.google.rpc.DebugInfo;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.serving.v1.ServingStoreGrpc;
import io.numaproj.numaflow.serving.v1.Store;
import io.numaproj.numaflow.shared.ExceptionUtils;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Service implements the proto generated server interface.
 */
@Slf4j
@AllArgsConstructor
class Service extends ServingStoreGrpc.ServingStoreImplBase {
    private final ServingStorer servingStore;
    private final CompletableFuture<Void> shutdownSignal;

    /**
     * Handles the Put RPC to store the payloads.
     *
     * @param request the PutRequest containing the ID and payloads
     * @param responseObserver the observer to send the response
     */
    @Override
    public void put(Store.PutRequest request, StreamObserver<Store.PutResponse> responseObserver) {
        // Convert gRPC payloads to internal Payload objects
        List<Payload> payloads = new ArrayList<>(request.getPayloadsCount());
        for (Store.Payload payload : request.getPayloadsList()) {
            payloads.add(new Payload(payload.getOrigin(), payload.getValue().toByteArray()));
        }

        try {
            // Store the payloads using the ServingStorer
            servingStore.put(new PutDatumImpl(request.getId(), payloads));
        } catch (Exception e) {
            // Build gRPC Status
            com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                    .setCode(Code.INTERNAL.getNumber())
                    .setMessage(ExceptionUtils.getExceptionErrorString() + ": "
                            + (e.getMessage() != null ? e.getMessage() : ""))
                    .addDetails(Any.pack(DebugInfo.newBuilder()
                            .setDetail(ExceptionUtils.getStackTrace(e))
                            .build()))
                    .build();
            responseObserver.onError(StatusProto.toStatusRuntimeException(status));
            shutdownSignal.completeExceptionally(e);
            return;
        }

        // Send the response
        responseObserver.onNext(Store.PutResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    /**
     * Handles the Get RPC to retrieve the stored data.
     *
     * @param request the GetRequest containing the ID
     * @param responseObserver the observer to send the response
     */
    @Override
    public void get(Store.GetRequest request, StreamObserver<Store.GetResponse> responseObserver) {
        // Retrieve the stored result using the ServingStorer
        StoredResult storedResult = null;
        try {
            storedResult = servingStore.get(new GetDatumImpl(request.getId()));
        } catch (Exception e) {
            // Build gRPC Status
            com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                    .setCode(Code.INTERNAL.getNumber())
                    .setMessage(ExceptionUtils.getExceptionErrorString() + ": "
                            + (e.getMessage() != null ? e.getMessage() : ""))
                    .addDetails(Any.pack(DebugInfo.newBuilder()
                            .setDetail(ExceptionUtils.getStackTrace(e))
                            .build()))
                    .build();
            responseObserver.onError(StatusProto.toStatusRuntimeException(status));
            shutdownSignal.completeExceptionally(e);
            return;
        }

        // Convert internal Payload objects to gRPC payloads
        List<Store.Payload> payloads = new ArrayList<>(storedResult.getPayloads().size());
        for (Payload payload : storedResult.getPayloads()) {
            payloads.add(Store.Payload.newBuilder()
                    .setOrigin(payload.getOrigin())
                    .setValue(com.google.protobuf.ByteString.copyFrom(payload.getValue()))
                    .build());
        }

        // Send the response
        responseObserver.onNext(Store.GetResponse.newBuilder()
                .setId(request.getId())
                .addAllPayloads(payloads)
                .build());
        responseObserver.onCompleted();
    }

    /**
     * Handles the IsReady RPC to indicate that the server is ready.
     *
     * @param request the Empty request
     * @param responseObserver the observer to send the response
     */
    @Override
    public void isReady(Empty request, StreamObserver<Store.ReadyResponse> responseObserver) {
        // Send the response indicating the server is ready
        responseObserver.onNext(Store.ReadyResponse.newBuilder().setReady(true).build());
        responseObserver.onCompleted();
    }
}
