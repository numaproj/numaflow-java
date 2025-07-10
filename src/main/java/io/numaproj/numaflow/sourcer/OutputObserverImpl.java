package io.numaproj.numaflow.sourcer;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.source.v1.SourceOuterClass;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * OutputObserverImpl is the implementation of the OutputObserver interface.
 * It is used to send messages to the gRPC client when the send method is called.
 */
@AllArgsConstructor
class OutputObserverImpl implements OutputObserver {
    StreamObserver<SourceOuterClass.ReadResponse> responseObserver;

    @Override
    public void send(Message message) {
        SourceOuterClass.ReadResponse response = buildResponse(message);
        responseObserver.onNext(response);
    }

    private SourceOuterClass.ReadResponse buildResponse(Message message) {
        SourceOuterClass.ReadResponse.Builder builder = SourceOuterClass.ReadResponse
                .newBuilder()
                .setResult(SourceOuterClass.ReadResponse.Result.newBuilder()
                        .addAllKeys(message.getKeys()
                                == null ? new ArrayList<>() : Arrays.asList(message.getKeys()))
                        .setPayload(
                                message.getValue() == null ? ByteString.EMPTY : ByteString.copyFrom(
                                        message.getValue()))
                        .setEventTime(Timestamp.newBuilder()
                                .setSeconds(message
                                        .getEventTime()
                                        .getEpochSecond())
                                .setNanos(message.getEventTime().getNano()))
                        .setOffset(SourceOuterClass.Offset.newBuilder()
                                .setOffset(message.getOffset().getValue()
                                        == null ? ByteString.EMPTY : ByteString.copyFrom(message
                                        .getOffset()
                                        .getValue()))
                                .setPartitionId(message.getOffset().getPartitionId()))
                        .putAllHeaders(message.getHeaders()
                                != null ? message.getHeaders() : new HashMap<>())
                        .build());

        return builder.build();
    }
}
