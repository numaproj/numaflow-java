package io.numaproj.numaflow.function.handlers;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.function.interfaces.Datum;
import io.numaproj.numaflow.function.types.Message;
import io.numaproj.numaflow.function.v1.Udfunction.DatumResponse;

import java.util.ArrayList;
import java.util.List;

/**
 * MapHandler exposes method for performing map streaming operation.
 * Implementations should override the processMessage method
 * which will be used for processing the input messages
 */

public abstract class MapStreamHandler {
    /**
     *
     * method which will be used for processing streaming messages.
     * stream observer need to be invoked on each message processed.
     *
     * @param keys message keys
     * @param datum current message to be processed
     * @param streamObserver stream observer of the response
     */
    public abstract void processMessage(String[] keys, Datum datum, StreamObserver<DatumResponse> streamObserver);

    /**
     * method will be utilized inside custom processMessage to invoke next on StreamObserver.
     *
     * @param message message
     * @return DatumResponse will be passed on to StreamObserver.
     */
    private DatumResponse buildDatumResponse(Message message) {
        return DatumResponse.newBuilder()
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
    protected void onNext(Message message, StreamObserver<DatumResponse> streamObserver) {
        streamObserver.onNext(buildDatumResponse(message));
    }
}
