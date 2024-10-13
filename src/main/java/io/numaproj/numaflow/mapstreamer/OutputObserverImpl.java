package io.numaproj.numaflow.mapstreamer;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.map.v1.MapOuterClass;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * OutputObserverImpl is the implementation of the OutputObserver interface.
 * It is used to send messages to the gRPC client when the send method is called.
 */
@AllArgsConstructor
class OutputObserverImpl implements OutputObserver {
    StreamObserver<MapOuterClass.MapResponse> responseObserver;

    @Override
    public void send(Message message) {
        MapOuterClass.MapResponse response = buildResponse(message);
        responseObserver.onNext(response);
    }

    private MapOuterClass.MapResponse buildResponse(Message message) {
        return MapOuterClass.MapResponse.newBuilder()
                .addResults(MapOuterClass.MapResponse.Result.newBuilder()
                        .setValue(
                                message.getValue() == null ? ByteString.EMPTY : ByteString.copyFrom(
                                        message.getValue()))
                        .addAllKeys(message.getKeys()
                                == null ? new ArrayList<>() : List.of(message.getKeys()))
                        .addAllTags(message.getTags()
                                == null ? new ArrayList<>() : List.of(message.getTags()))
                        .build()).build();
    }
}
