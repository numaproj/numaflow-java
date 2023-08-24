package io.numaproj.numaflow.sideinput;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sideinput.v1.SideInputGrpc;
import io.numaproj.numaflow.sideinput.v1.Sideinput;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;



@Slf4j
@AllArgsConstructor
class Service extends SideInputGrpc.SideInputImplBase {

    private final SideInputRetriever sideInputRetriever;

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


        // process request
        Message message = sideInputRetriever.retrieveSideInput();

        // set response
        responseObserver.onNext(buildResponse(message));
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
                .setValue(message.getValue() == null ? ByteString.EMPTY : ByteString.copyFrom(
                        message.getValue()))
                .build();
    }

}
