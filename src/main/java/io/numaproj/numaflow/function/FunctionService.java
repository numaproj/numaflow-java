package io.numaproj.numaflow.function;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.function.v1.Udfunction;
import io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc;

import static io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc.getMapFnMethod;

// TODO: implement reduceFn
class FunctionService extends UserDefinedFunctionGrpc.UserDefinedFunctionImplBase {
  private final MapHandler mapHandler;

  public FunctionService(MapHandler mapHandler) {
    this.mapHandler = mapHandler;
  }

  /**
   * Applies a function to each datum element.
   */
  @Override
  public void mapFn(Udfunction.Datum request, StreamObserver<Udfunction.DatumList> responseObserver) {
    if (this.mapHandler == null) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getMapFnMethod(), responseObserver);
      return;
    }

    // get key from gPRC metadata
    String key = Function.DATUM_CONTEXT_KEY.get();

    // get Datum from request
    Udfunction.Datum handlerDatum = Udfunction.Datum.newBuilder()
        .setValue(request.getValue())
        .setEventTime(request.getEventTime())
        .setWatermark(request.getWatermark())
        .build();

    // process Datum
    Message[] messages = mapHandler.HandleDo(key, handlerDatum);

    // create response
    Udfunction.DatumList.Builder datumListBuilder = Udfunction.DatumList.newBuilder();
    for (Message message : messages) {
      Udfunction.Datum d = Udfunction.Datum.newBuilder()
          .setKey(message.getKey())
          .setValue(ByteString.copyFrom(message.getValue()))
          .build();

      datumListBuilder.addElements(d);
    }

    // set response
    responseObserver.onNext(datumListBuilder.build());
    responseObserver.onCompleted();
  }

  /**
   * IsReady is the heartbeat endpoint for gRPC.
   */
  @Override
  public void isReady(Empty request, StreamObserver<Udfunction.ReadyResponse> responseObserver) {
    responseObserver.onNext(Udfunction.ReadyResponse.newBuilder().setReady(true).build());
    responseObserver.onCompleted();
  }
}
