package io.numaproj.numaflow.mapstreamer;

import akka.actor.ActorRef;

/**
 * Implementation of the OutputObserver interface.
 * It sends messages to the supervisor actor when the send method is called.
 */
public class OutputObserverImpl implements OutputObserver {
    private final ActorRef supervisorActor;

    public OutputObserverImpl(ActorRef supervisorActor) {
        this.supervisorActor = supervisorActor;
    }

    @Override
    public void send(Message message) {
        supervisorActor.tell(message, ActorRef.noSender());
    }
}
