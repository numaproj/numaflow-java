package io.numaproj.numaflow.mapper;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.map.v1.MapGrpc;
import io.numaproj.numaflow.map.v1.MapOuterClass;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static io.numaproj.numaflow.map.v1.MapGrpc.getMapFnMethod;

@Slf4j
@AllArgsConstructor
class Service extends MapGrpc.MapImplBase {

    private final Mapper mapper;

    /**
     * Applies a function to each datum element.
     */
    @Override
    public void mapFn(
            MapOuterClass.MapRequest request,
            StreamObserver<MapOuterClass.MapResponse> responseObserver) {

        if (this.mapper == null) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(
                    getMapFnMethod(),
                    responseObserver);
            return;
        }

        HandlerDatum handlerDatum = new HandlerDatum(
                request.getValue().toByteArray(),
                Instant.ofEpochSecond(
                        request.getWatermark().getSeconds(),
                        request.getWatermark().getNanos()),
                Instant.ofEpochSecond(
                        request.getEventTime().getSeconds(),
                        request.getEventTime().getNanos())
        );

        // process request
        MessageList messageList = mapper.processMessage(request
                .getKeysList()
                .toArray(new String[0]), handlerDatum);

        // set response
        responseObserver.onNext(buildResponse(messageList));
        responseObserver.onCompleted();
    }

    /**
     * IsReady is the heartbeat endpoint for gRPC.
     */
    @Override
    public void isReady(
            Empty request,
            StreamObserver<MapOuterClass.ReadyResponse> responseObserver) {
        responseObserver.onNext(MapOuterClass.ReadyResponse.newBuilder().setReady(true).build());
        responseObserver.onCompleted();
    }

    private MapOuterClass.MapResponse buildResponse(MessageList messageList) {
        MapOuterClass.MapResponse.Builder responseBuilder = MapOuterClass
                .MapResponse
                .newBuilder();

        messageList.getMessages().forEach(message -> {
            responseBuilder.addResults(MapOuterClass.MapResponse.Result.newBuilder()
                    .setValue(message.getValue() == null ? ByteString.EMPTY : ByteString.copyFrom(
                            message.getValue()))
                    .addAllKeys(message.getKeys()
                            == null ? new ArrayList<>() : List.of(message.getKeys()))
                    .addAllTags(message.getTags()
                            == null ? new ArrayList<>() : List.of(message.getTags()))
                    .build());
        });
        return responseBuilder.build();
    }

}
