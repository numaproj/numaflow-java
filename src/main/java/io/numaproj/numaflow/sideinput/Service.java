package io.numaproj.numaflow.sideinput;

import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.google.rpc.Code;
import com.google.rpc.DebugInfo;
import io.grpc.protobuf.StatusProto;
import io.numaproj.numaflow.shared.ExceptionUtils;
import io.grpc.stub.StreamObserver;
import com.google.protobuf.ByteString;
import io.numaproj.numaflow.sideinput.v1.SideInputGrpc;
import io.numaproj.numaflow.sideinput.v1.Sideinput;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
@AllArgsConstructor
class Service extends SideInputGrpc.SideInputImplBase {

    private final SideInputRetriever sideInputRetriever;
    private final CompletableFuture<Void> shutdownSignal;

    /**
     * Invokes the side input retriever to retrieve side input.
     */
    @Override
    public void retrieveSideInput(
            Empty request,
            StreamObserver<Sideinput.SideInputResponse> responseObserver) {

        if (this.sideInputRetriever == null) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(
                    SideInputGrpc.getRetrieveSideInputMethod(),
                    responseObserver);
            return;
        }
        try {
            // process request
            Message message = sideInputRetriever.retrieveSideInput();
            // set response
            responseObserver.onNext(buildResponse(message));

        } catch (Exception e) {
            log.error("Encountered error in retrieveSideInput", e);
            shutdownSignal.completeExceptionally(e);
            // Build gRPC Status [error]
            com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                    .setCode(Code.INTERNAL.getNumber())
                    .setMessage(
                            ExceptionUtils.ERR_SIDE_INPUT_EXCEPTION + ": "
                                    + (e.getMessage() != null ? e.getMessage() : ""))
                    .addDetails(Any.pack(DebugInfo.newBuilder()
                            .setDetail(ExceptionUtils.getStackTrace(e))
                            .build()))
                    .build();
            responseObserver.onError(StatusProto.toStatusRuntimeException(status));
            return;
        }
        responseObserver.onCompleted();
    }

    /**
     * IsReady is the heartbeat endpoint for gRPC.
     */
    @Override
    public void isReady(Empty request, StreamObserver<Sideinput.ReadyResponse> responseObserver) {
        responseObserver.onNext(Sideinput.ReadyResponse.newBuilder().setReady(true).build());
        responseObserver.onCompleted();
    }

    private Sideinput.SideInputResponse buildResponse(Message message) {
        return Sideinput.SideInputResponse.newBuilder()
                .setValue(message.getValue() == null ? ByteString.EMPTY
                        : ByteString.copyFrom(
                                message.getValue()))
                .setNoBroadcast(message.isNoBroadcast())
                .build();
    }

}
