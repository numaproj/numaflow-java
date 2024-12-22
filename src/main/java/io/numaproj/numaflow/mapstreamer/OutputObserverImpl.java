package io.numaproj.numaflow.mapstreamer;

import akka.actor.ActorRef;
import com.google.protobuf.ByteString;
import io.numaproj.numaflow.map.v1.MapOuterClass;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of the OutputObserver interface.
 * It sends messages to the supervisor actor when the send method is called.
 */
public class OutputObserverImpl implements OutputObserver {
    private final ActorRef supervisorActor;
    private final String requestID;

    public OutputObserverImpl(ActorRef supervisorActor, String requestID) {
        this.supervisorActor = supervisorActor;
        this.requestID = requestID;
    }

    @Override
    public void send(Message message) {
        MapOuterClass.MapResponse response = MapOuterClass.MapResponse.newBuilder()
                .setId(requestID)
                .addResults(MapOuterClass.MapResponse.Result.newBuilder()
                        .setValue(
                                message.getValue() == null ? ByteString.EMPTY : ByteString.copyFrom(
                                        message.getValue()))
                        .addAllKeys(message.getKeys()
                                == null ? new ArrayList<>() : List.of(message.getKeys()))
                        .addAllTags(message.getTags()
                                == null ? new ArrayList<>() : List.of(message.getTags()))
                        .build()).build();
        supervisorActor.tell(response, ActorRef.noSender());
    }
}
