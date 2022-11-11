package io.numaproj.numaflow.sink;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sink.v1.Udsink;
import io.numaproj.numaflow.sink.v1.UserDefinedSinkGrpc;

import static io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc.getMapFnMethod;

class SinkService extends UserDefinedSinkGrpc.UserDefinedSinkImplBase {
    private final SinkHandler sinkHandler;

    public SinkService(SinkHandler sinkHandler) {
        this.sinkHandler = sinkHandler;
    }

    /**
     * Applies a function to each datum element.
     */
    public void sinkFn(Udsink.DatumList request, StreamObserver<Udsink.ResponseList> responseObserver) {
        if (this.sinkHandler == null) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMapFnMethod(), responseObserver);
            return;
        }

        // get DatumList from request
        var numElem = request.getElementsCount();
        var handlerDatumList = new Udsink.Datum[numElem];
        for (int i = 0; i < numElem; i++) {
            var d = request.getElements(i);
            handlerDatumList[i] = Udsink.Datum.newBuilder()
                    .setId(d.getId())
                    .setValue(d.getValue())
                    .setEventTime(d.getEventTime())
                    .setWatermark(d.getWatermark())
                    .build();
        }

        // process DatumList
        Response[] responses = sinkHandler.HandleDo(handlerDatumList);

        // create response
        var responseListBuilder = Udsink.ResponseList.newBuilder();
        for (Response response : responses) {
            Udsink.Response r = Udsink.Response.newBuilder()
                    .setId(response.getId())
                    .setSuccess(response.getSuccess())
                    .setErrMsg(response.getErr())
                    .build();

            responseListBuilder.addResponses(r);
        }

        // set response
        responseObserver.onNext(responseListBuilder.build());
        responseObserver.onCompleted();
    }

    /**
     * IsReady is the heartbeat endpoint for gRPC.
     */
    @Override
    public void isReady(Empty request, StreamObserver<Udsink.ReadyResponse> responseObserver) {
        responseObserver.onNext(Udsink.ReadyResponse.newBuilder().setReady(true).build());
        responseObserver.onCompleted();
    }
}
