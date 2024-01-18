package io.numaproj.numaflow.reducestreamer;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ChildRestartStats;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import akka.japi.pf.ReceiveBuilder;
import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import io.numaproj.numaflow.reducestreamer.model.HandlerDatum;
import io.numaproj.numaflow.reducestreamer.model.Metadata;
import io.numaproj.numaflow.reducestreamer.user.ReduceStreamer;
import io.numaproj.numaflow.reducestreamer.user.ReduceStreamerFactory;
import lombok.extern.slf4j.Slf4j;
import scala.PartialFunction;
import scala.collection.Iterable;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * ReduceSupervisorActor actor distributes the messages to actors and handles failure.
 */
@Slf4j
class ReduceSupervisorActor extends AbstractActor {
    private final ReduceStreamerFactory<? extends ReduceStreamer> reduceStreamerFactory;
    private final Metadata md;
    private final ActorRef shutdownActor;
    private final ActorRef responseStreamActor;
    private final StreamObserver<ReduceOuterClass.ReduceResponse> responseObserver;
    private final Map<String, ActorRef> actorsMap = new HashMap<>();

    public ReduceSupervisorActor(
            ReduceStreamerFactory<? extends ReduceStreamer> reduceStreamerFactory,
            Metadata md,
            ActorRef shutdownActor,
            ActorRef responseStreamActor,
            StreamObserver<ReduceOuterClass.ReduceResponse> responseObserver) {
        this.reduceStreamerFactory = reduceStreamerFactory;
        this.md = md;
        this.shutdownActor = shutdownActor;
        this.responseStreamActor = responseStreamActor;
        this.responseObserver = responseObserver;
    }

    public static Props props(
            ReduceStreamerFactory<? extends ReduceStreamer> reduceStreamerFactory,
            Metadata md,
            ActorRef shutdownActor,
            ActorRef responseStreamActor,
            StreamObserver<ReduceOuterClass.ReduceResponse> responseObserver) {
        return Props.create(
                ReduceSupervisorActor.class,
                reduceStreamerFactory,
                md,
                shutdownActor,
                responseStreamActor,
                responseObserver);
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
                .match(ActorRequest.class, this::invokeActors)
                .match(String.class, this::sendEOF)
                .match(ActorResponse.class, this::handleActorResponse)
                .build();
    }

    /*
        based on the keys of the input message invoke the right reduce streamer actor
        if there is no actor for an incoming set of keys, create a new reduce streamer actor
        track all the child actors using actors map
     */
    private void invokeActors(ActorRequest actorRequest) {
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
        if (actorResponse.getType() == ActorResponseType.EOF_RESPONSE) {
            // forward the response to the response stream actor to send back to gRPC output stream.
            this.responseStreamActor.tell(actorResponse, getSelf());
        } else if (actorResponse.getType() == ActorResponseType.READY_FOR_CLEAN_UP_SIGNAL) {
            // the corresponding actor is ready to be cleaned up.
            // remove the child entry from the map.
            // if there are no entries in the map, that means processing is
            // done we can close the entire stream.
            actorsMap.remove(actorResponse.getActorUniqueIdentifier());
            if (actorsMap.isEmpty()) {
                responseObserver.onCompleted();
                getContext().getSystem().stop(getSelf());
            }
        } else {
            throw new RuntimeException(
                    "Supervisor actor received an actor response with unsupported type: "
                            + actorResponse.getType());
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
