package io.numaproj.numaflow.mapstreamer;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.mapstream.v1.Mapstream;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * OutputObserverImpl is the implementation of the OutputObserver interface.
 * It is used to send messages to the gRPC client when the send method is called.
 */
@AllArgsConstructor
class OutputObserverImpl implements OutputObserver {
    StreamObserver<Mapstream.MapStreamResponse> responseObserver;

    @Override
    public void send(Message message) {
        Mapstream.MapStreamResponse response = buildResponse(message);
        responseObserver.onNext(response);
    }

    private Mapstream.MapStreamResponse buildResponse(Message message) {
        return Mapstream.MapStreamResponse.newBuilder()
                .setResult(Mapstream.MapStreamResponse.Result.newBuilder()
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
