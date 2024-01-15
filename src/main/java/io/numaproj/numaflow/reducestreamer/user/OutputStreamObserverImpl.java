package io.numaproj.numaflow.reducestreamer.user;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass.ReduceResponse;
import io.numaproj.numaflow.reducestreamer.model.Message;
import io.numaproj.numaflow.reducestreamer.model.Metadata;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@AllArgsConstructor
public
class OutputStreamObserverImpl implements OutputStreamObserver {
    Metadata md;
    StreamObserver<ReduceResponse> responseObserver;

    @Override
    public void send(Message message) {
        ReduceResponse response = buildResponse(message);
        responseObserver.onNext(response);
    }

    private ReduceResponse buildResponse(Message message) {
        ReduceOuterClass.ReduceResponse.Builder responseBuilder = ReduceOuterClass.ReduceResponse.newBuilder();
        // set the window using the actor metadata.
        responseBuilder.setWindow(ReduceOuterClass.Window.newBuilder()
                .setStart(Timestamp.newBuilder()
                        .setSeconds(this.md.getIntervalWindow().getStartTime().getEpochSecond())
                        .setNanos(this.md.getIntervalWindow().getStartTime().getNano()))
                .setEnd(Timestamp.newBuilder()
                        .setSeconds(this.md.getIntervalWindow().getEndTime().getEpochSecond())
                        .setNanos(this.md.getIntervalWindow().getEndTime().getNano()))
                .setSlot("slot-0").build());
        responseBuilder.setEOF(false);
        // set the result.
        responseBuilder.setResult(ReduceOuterClass.ReduceResponse.Result
                .newBuilder()
                .setValue(ByteString.copyFrom(message.getValue()))
                .addAllKeys(message.getKeys()
                        == null ? new ArrayList<>():Arrays.asList(message.getKeys()))
                .addAllTags(
                        message.getTags() == null ? new ArrayList<>():List.of(message.getTags()))
                .build());

        return responseBuilder.build();
    }
}
