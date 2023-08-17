package io.numaproj.numaflow.mapstreamer;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.mapstream.v1.Mapstream;

import java.util.ArrayList;
import java.util.List;

/**
 * MapStreamer exposes method for performing map streaming operation.
 * Implementations should override the processMessage method
 * which will be used for processing the input messages
 */

public abstract class MapStreamer {
    /**
     *
     * method which will be used for processing streaming messages.
     * stream observer need to be invoked on each message processed.
     *
     * @param keys message keys
     * @param datum current message to be processed
     * @param streamObserver stream observer of the response
     */
    public abstract void processMessage(String[] keys, Datum datum, StreamObserver<Mapstream.MapStreamResponse.Result> streamObserver);

    /**
     * method will be utilized inside custom processMessage to invoke next on StreamObserver.
     *
     * @param message message
     * @return DatumResponse will be passed on to StreamObserver.
     */
    private Mapstream.MapStreamResponse.Result buildDatumResponse(Message message) {
        return Mapstream.MapStreamResponse.Result.newBuilder()
            .setValue(message.getValue() == null ? ByteString.EMPTY : ByteString.copyFrom(
                    message.getValue()))
            .addAllKeys(message.getKeys()
                    == null ? new ArrayList<>() : List.of(message.getKeys()))
            .addAllTags(message.getTags()
                    == null ? new ArrayList<>() : List.of(message.getTags()))
            .build();
    }

    /**
     * method will be utilized inside custom processMessage to invoke next on StreamObserver.
     *
     * @param message message
     * @param streamObserver stream observer of the response
     */
    protected void onNext(Message message, StreamObserver<Mapstream.MapStreamResponse.Result> streamObserver) {
        streamObserver.onNext(buildDatumResponse(message));
    }
}
