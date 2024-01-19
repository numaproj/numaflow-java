package io.numaproj.numaflow.reducestreamer;

import akka.actor.ActorRef;
import io.numaproj.numaflow.reducestreamer.model.Message;
import io.numaproj.numaflow.reducestreamer.model.OutputStreamObserver;
import lombok.AllArgsConstructor;

@AllArgsConstructor
class OutputStreamObserverImpl implements OutputStreamObserver {
    private final ActorRef responseStreamActor;

    @Override
    public void send(Message message) {
        this.responseStreamActor.tell(message, ActorRef.noSender());
    }
}
