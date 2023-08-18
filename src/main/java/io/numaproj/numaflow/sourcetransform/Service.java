package io.numaproj.numaflow.sourcetransform;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sourcetransformer.v1.SourceTransformGrpc;
import io.numaproj.numaflow.sourcetransformer.v1.Transform;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static io.numaproj.numaflow.map.v1.MapGrpc.getMapFnMethod;


@Slf4j
@AllArgsConstructor
class Service extends SourceTransformGrpc.SourceTransformImplBase {

    private final SourceTransform transformer;

    /**
     * Applies a function to each datum element.
     */
    @Override
    public void sourceTransformFn(
            Transform.SourceTransformRequest request,
            StreamObserver<Transform.SourceTransformResponse> responseObserver) {

        if (this.transformer == null) {
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
        MessageList messageList = this.transformer.processMessage(request
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
    public void isReady(Empty request, StreamObserver<Transform.ReadyResponse> responseObserver) {
        responseObserver.onNext(Transform.ReadyResponse.newBuilder().setReady(true).build());
        responseObserver.onCompleted();
    }

    private Transform.SourceTransformResponse buildResponse(MessageList messageList) {
        Transform.SourceTransformResponse.Builder responseBuilder = Transform
                .SourceTransformResponse
                .newBuilder();

        messageList.getMessages().forEach(message -> {
            responseBuilder.addResults(Transform.SourceTransformResponse.Result.newBuilder()
                    .setValue(message.getValue() == null ? ByteString.EMPTY : ByteString.copyFrom(
                            message.getValue()))
                            .setEventTime(Timestamp.newBuilder()
                                    .setSeconds(message
                                            .getEventTime()
                                            .getEpochSecond())
                                    .setNanos(message.getEventTime().getNano()))
                    .addAllKeys(message.getKeys()
                            == null ? new ArrayList<>() : List.of(message.getKeys()))
                    .addAllTags(message.getTags()
                            == null ? new ArrayList<>() : List.of(message.getTags()))
                    .build());
        });
        return responseBuilder.build();
    }

}
