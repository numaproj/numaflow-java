package io.numaproj.numaflow.sessionreducer;

import akka.actor.ActorRef;
import com.google.protobuf.ByteString;
import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;
import io.numaproj.numaflow.sessionreducer.model.Message;
import io.numaproj.numaflow.sessionreducer.model.OutputStreamObserver;
import lombok.AllArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * OutputStreamObserverImpl transforms a message to an ActorResponse.
 * The send method sends the ActorResponse to the output actor to forward to output gRPC stream.
 */
@AllArgsConstructor
class OutputStreamObserverImpl implements OutputStreamObserver {
    private final ActorRef outputActor;
    @Setter
    private Sessionreduce.KeyedWindow keyedWindow;

    @Override
    public void send(Message message) {
        this.outputActor.tell(
                buildResponse(message, this.keyedWindow),
                ActorRef.noSender());
    }

    private ActorResponse buildResponse(Message message, Sessionreduce.KeyedWindow keyedWindow) {
        Sessionreduce.SessionReduceResponse.Builder responseBuilder = Sessionreduce.SessionReduceResponse.newBuilder();
        // set the window
        responseBuilder.setKeyedWindow(keyedWindow);
        // set EOF to false
        responseBuilder.setEOF(false);
        // set the result.
        responseBuilder.setResult(Sessionreduce.SessionReduceResponse.Result
                .newBuilder()
                .setValue(ByteString.copyFrom(message.getValue()))
                .addAllKeys(message.getKeys()
                        == null ? new ArrayList<>() : Arrays.asList(message.getKeys()))
                .addAllTags(
                        message.getTags() == null ? new ArrayList<>() : List.of(message.getTags()))
                .build());
        return ActorResponse.builder()
                .response(responseBuilder.build())
                .build();
    }
}
