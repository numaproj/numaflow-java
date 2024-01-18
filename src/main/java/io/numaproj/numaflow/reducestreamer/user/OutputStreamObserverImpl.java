package io.numaproj.numaflow.reducestreamer.user;

import akka.actor.ActorRef;
import io.numaproj.numaflow.reducestreamer.model.Message;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public
class OutputStreamObserverImpl implements OutputStreamObserver {
    private final ActorRef responseStreamActor;

    @Override
    public void send(Message message) {
        this.responseStreamActor.tell(message, ActorRef.noSender());
    }
}
