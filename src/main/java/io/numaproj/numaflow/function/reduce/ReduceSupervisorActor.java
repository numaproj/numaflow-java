package io.numaproj.numaflow.function.reduce;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ChildRestartStats;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import akka.japi.pf.ReceiveBuilder;
import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.function.Function;
import io.numaproj.numaflow.function.FunctionService;
import io.numaproj.numaflow.function.HandlerDatum;
import io.numaproj.numaflow.function.HandlerDatumMetadata;
import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.v1.Udfunction;
import lombok.extern.slf4j.Slf4j;
import scala.PartialFunction;
import scala.collection.Iterable;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Supervisor actor distributes the messages to actors and handles failure.
 */

@Slf4j
public class ReduceSupervisorActor extends AbstractActor {
    private final ReducerFactory<? extends ReduceHandler> reducerFactory;
    private final Metadata md;
    private final ActorRef shutdownActor;
    private final StreamObserver<Udfunction.DatumResponseList> responseObserver;
    private final Map<String, ActorRef> actorsMap = new HashMap<>();

    public ReduceSupervisorActor(
            ReducerFactory<? extends ReduceHandler> reducerFactory,
            Metadata md,
            ActorRef shutdownActor,
            StreamObserver<Udfunction.DatumResponseList> responseObserver) {
        this.reducerFactory = reducerFactory;
        this.md = md;
        this.shutdownActor = shutdownActor;
        this.responseObserver = responseObserver;
    }

    public static Props props(
            ReducerFactory<? extends ReduceHandler> reducerFactory,
            Metadata md,
            ActorRef shutdownActor,
            StreamObserver<Udfunction.DatumResponseList> responseObserver) {
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
        FunctionService.actorSystem.stop(getSelf());
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return new ReduceSupervisorStrategy();
    }


    @Override
    public void postStop() {
        log.debug("post stop of supervisor executed - {}", getSelf().toString());
        shutdownActor.tell(Function.SUCCESS, ActorRef.noSender());
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder
                .create()
                .match(Udfunction.DatumRequest.class, this::invokeActors)
                .match(String.class, this::sendEOF)
                .match(ActorResponse.class, this::responseListener)
                .build();
    }

    /*
        based on the keys of the input message invoke the right actor
        if there is no actor for an incoming set of keys, create a new actor
        track all the child actors using actors map
     */
    private void invokeActors(Udfunction.DatumRequest datumRequest) {
        String[] keys = datumRequest.getKeysList().toArray(new String[0]);
        String keyStr = String.join(Function.DELIMITTER, keys);
        if (!actorsMap.containsKey(keyStr)) {
            ReduceHandler reduceHandler = reducerFactory.createReducer();
            ActorRef actorRef = getContext()
                    .actorOf(ReduceActor.props(keys, md, reduceHandler));

            actorsMap.put(keyStr, actorRef);
        }
        HandlerDatum handlerDatum = constructHandlerDatum(datumRequest);
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

        responseObserver.onNext(actorResponse.getDatumList());
        actorsMap.remove(String.join(Function.DELIMITTER, actorResponse.getKeys()));
        if (actorsMap.isEmpty()) {
            responseObserver.onCompleted();
            getContext().getSystem().stop(getSelf());
        }
    }

    private HandlerDatum constructHandlerDatum(Udfunction.DatumRequest datumRequest) {
        HandlerDatumMetadata handlerDatumMetadata = new HandlerDatumMetadata(
                datumRequest.getMetadata().getId(),
                datumRequest.getMetadata().getNumDelivered()
        );
        return new HandlerDatum(
                datumRequest.getValue().toByteArray(),
                Instant.ofEpochSecond(
                        datumRequest.getWatermark().getWatermark().getSeconds(),
                        datumRequest.getWatermark().getWatermark().getNanos()),
                Instant.ofEpochSecond(
                        datumRequest.getEventTime().getEventTime().getSeconds(),
                        datumRequest.getEventTime().getEventTime().getNanos()),
                handlerDatumMetadata
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
