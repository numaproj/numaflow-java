package io.numaproj.numaflow.reducer;

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
    private final ReducerFactory<? extends Reducer> reducerFactory;
    private final Metadata md;
    private final ActorRef shutdownActor;
    private final StreamObserver<ReduceOuterClass.ReduceResponse> responseObserver;
    private final Map<String, ActorRef> actorsMap = new HashMap<>();

    public ReduceSupervisorActor(
            ReducerFactory<? extends Reducer> reducerFactory,
            Metadata md,
            ActorRef shutdownActor,
            StreamObserver<ReduceOuterClass.ReduceResponse> responseObserver) {
        this.reducerFactory = reducerFactory;
        this.md = md;
        this.shutdownActor = shutdownActor;
        this.responseObserver = responseObserver;
    }

    public static Props props(
            ReducerFactory<? extends Reducer> reducerFactory,
            Metadata md,
            ActorRef shutdownActor,
            StreamObserver<ReduceOuterClass.ReduceResponse> responseObserver) {
        return Props.create(
                ReduceSupervisorActor.class,
                reducerFactory,
                md,
                shutdownActor,
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
                .match(ReduceOuterClass.ReduceRequest.class, this::invokeActors)
                .match(String.class, this::sendEOF)
                .match(ActorResponse.class, this::responseListener)
                .build();
    }

    /*
        based on the keys of the input message invoke the right actor
        if there is no actor for an incoming set of keys, create a new actor
        track all the child actors using actors map
     */
    private void invokeActors(ReduceOuterClass.ReduceRequest reduceRequest) {
        ReduceOuterClass.ReduceRequest.Payload payload = reduceRequest.getPayload();
        String[] keys = payload.getKeysList().toArray(new String[0]);
        // TODO - do we need to include window information in the keyStr?
        // for aligned reducer, there is always single window.
        // but at the same time, would like to be consistent with GO SDK implementation.
        String keyStr = String.join(Constants.DELIMITER, keys);
        if (!actorsMap.containsKey(keyStr)) {
            Reducer reduceHandler = reducerFactory.createReducer();
            ActorRef actorRef = getContext()
                    .actorOf(ReduceActor.props(keys, md, reduceHandler));
            actorsMap.put(keyStr, actorRef);
        }

        HandlerDatum handlerDatum = constructHandlerDatum(payload);
        actorsMap.get(keyStr).tell(handlerDatum, getSelf());
    }

    private void sendEOF(String EOF) {
        for (Map.Entry<String, ActorRef> entry : actorsMap.entrySet()) {
            entry.getValue().tell(EOF, getSelf());
        }
    }

    // listen to child actors for the result.
    private void responseListener(ActorResponse actorResponse) {
        /*
            send the result back to the client
            remove the child entry from the map after getting result.
            if there are no entries in the map, that means processing is
            done we can close the stream.
         */

        responseObserver.onNext(actorResponse.getResponse());
        // TODO - do we need to include window information for aligned windows?
        // for aligned reducer, there is always single window.
        // but at the same time, would like to be consistent with GO SDK implementation.
        actorsMap.remove(String.join(Constants.DELIMITER, actorResponse.getKeys()));
        if (actorsMap.isEmpty()) {
            responseObserver.onCompleted();
            getContext().getSystem().stop(getSelf());
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
