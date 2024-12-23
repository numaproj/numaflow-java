package io.numaproj.numaflow.mapstreamer;

import akka.actor.ActorRef;
import com.google.protobuf.ByteString;
import io.numaproj.numaflow.map.v1.MapOuterClass;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of the OutputObserver interface.
 * It sends messages to the supervisor actor when the send method is called.
 * <p>
 * We create a new output observer for every map stream invocation, but they
 * all forward the response to a common actor (supervisor) who will send the
 * responses back to the client. We cannot directly write to the gRPC stream
 * from the output observer because the gRPC stream observer is not thread
 * safe, whereas writing to an actor is thread safe, and only one actor will
 * write the responses back to the client.
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

    public void sendEOF() {
        supervisorActor.tell(MapOuterClass.MapResponse
                .newBuilder()
                .setId(requestID)
                .setStatus(MapOuterClass.TransmissionStatus.newBuilder().setEot(true).build())
                .build(), ActorRef.noSender());
    }
}
