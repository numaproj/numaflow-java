package io.numaproj.numaflow.reducestreamer;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ChildRestartStats;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import akka.japi.pf.ReceiveBuilder;
import com.google.common.base.Preconditions;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import io.numaproj.numaflow.reducestreamer.model.Metadata;
import io.numaproj.numaflow.reducestreamer.model.ReduceStreamer;
import io.numaproj.numaflow.reducestreamer.model.ReduceStreamerFactory;
import lombok.extern.slf4j.Slf4j;
import scala.PartialFunction;
import scala.collection.Iterable;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Reduce supervisor actor distributes the messages to other actors and handles failures.
 */
@Slf4j
class ReduceSupervisorActor extends AbstractActor {
    private final ReduceStreamerFactory<? extends ReduceStreamer> reduceStreamerFactory;
    private final Metadata md;
    private final ActorRef shutdownActor;
    private final ActorRef responseStreamActor;
    private final Map<String, ActorRef> actorsMap = new HashMap<>();

    public ReduceSupervisorActor(
            ReduceStreamerFactory<? extends ReduceStreamer> reduceStreamerFactory,
            Metadata md,
            ActorRef shutdownActor,
            ActorRef responseStreamActor) {
        this.reduceStreamerFactory = reduceStreamerFactory;
        this.md = md;
        this.shutdownActor = shutdownActor;
        this.responseStreamActor = responseStreamActor;
    }

    public static Props props(
            ReduceStreamerFactory<? extends ReduceStreamer> reduceStreamerFactory,
            Metadata md,
            ActorRef shutdownActor,
            ActorRef responseStreamActor) {
        return Props.create(
                ReduceSupervisorActor.class,
                reduceStreamerFactory,
                md,
                shutdownActor,
                responseStreamActor);
    }

    // if there is an uncaught exception stop in the supervisor actor, send a signal to shut down
    @Override
    public void preRestart(Throwable reason, Optional<Object> message) {
        log.debug("supervisor pre restart was executed");
        shutdownActor.tell(reason, ActorRef.noSender());
        Service.reduceActorSystem.stop(getSelf());
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return new ReduceSupervisorStrategy();
    }


    @Override
    public void postStop() {
        log.debug("post stop of supervisor executed - {}", getSelf().toString());
        shutdownActor.tell(Constants.SUCCESS, ActorRef.noSender());
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder
                .create()
                .match(ActorRequest.class, this::invokeActor)
                .match(String.class, this::sendEOF)
                .match(ActorResponse.class, this::handleActorResponse)
                .build();
    }

    /*
        based on the keys of the input message invoke the right reduce streamer actor
        if there is no actor for an incoming set of keys, create a new reduce streamer actor
        track all the child actors using actors map
     */
    private void invokeActor(ActorRequest actorRequest) {
        String[] keys = actorRequest.getKeySet();
        String uniqueId = actorRequest.getUniqueIdentifier();
        if (!actorsMap.containsKey(uniqueId)) {
            ReduceStreamer reduceStreamerHandler = reduceStreamerFactory.createReduceStreamer();
            ActorRef actorRef = getContext()
                    .actorOf(ReduceStreamerActor.props(
                            keys,
                            this.md,
                            reduceStreamerHandler,
                            this.responseStreamActor));
            actorsMap.put(uniqueId, actorRef);
        }
        HandlerDatum handlerDatum = constructHandlerDatum(actorRequest.getRequest().getPayload());
        actorsMap.get(uniqueId).tell(handlerDatum, getSelf());
    }

    private void sendEOF(String EOF) {
        for (Map.Entry<String, ActorRef> entry : actorsMap.entrySet()) {
            entry.getValue().tell(EOF, getSelf());
        }
    }

    private void handleActorResponse(ActorResponse actorResponse) {
        // when the supervisor receives an actor response, it means the corresponding
        // reduce streamer actor has finished its job.
        // we remove the entry from the actors map.
        actorsMap.remove(actorResponse.getActorUniqueIdentifier());
        if (actorsMap.isEmpty()) {
            // since the actors map is empty, this particular actor response is the last response to forward to output gRPC stream.
            actorResponse.setLast(true);
            this.responseStreamActor.tell(actorResponse, getSelf());
        } else {
            this.responseStreamActor.tell(actorResponse, getSelf());
        }
    }

    private HandlerDatum constructHandlerDatum(ReduceOuterClass.ReduceRequest.Payload payload) {
        return new HandlerDatum(
                payload.getValue().toByteArray(),
                Instant.ofEpochSecond(
                        payload.getWatermark().getSeconds(),
                        payload.getWatermark().getNanos()),
                Instant.ofEpochSecond(
                        payload.getEventTime().getSeconds(),
                        payload.getEventTime().getNanos())
        );
    }

    /*
        We need supervisor to handle failures, by default if there are any failures
        actors will be restarted, but we want to escalate the exception and terminate
        the system.
    */
    private final class ReduceSupervisorStrategy extends SupervisorStrategy {

        @Override
        public PartialFunction<Throwable, Directive> decider() {
            return DeciderBuilder.match(Exception.class, e -> SupervisorStrategy.stop()).build();
        }

        @Override
        public void handleChildTerminated(
                akka.actor.ActorContext context,
                ActorRef child,
                Iterable<ActorRef> children) {

        }

        @Override
        public void processFailure(
                akka.actor.ActorContext context,
                boolean restart,
                ActorRef child,
                Throwable cause,
                ChildRestartStats stats,
                Iterable<ChildRestartStats> children) {

            Preconditions.checkArgument(
                    !restart,
                    "on failures, we will never restart our actors, we escalate");
            /*
                   tell the shutdown actor about the exception.
             */
            log.debug("process failure of supervisor strategy executed - {}", getSelf().toString());
            shutdownActor.tell(cause, context.parent());
        }
    }
}
