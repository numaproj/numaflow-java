package io.numaproj.numaflow.accumulator;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.AllForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import akka.japi.pf.ReceiveBuilder;
import io.numaproj.numaflow.accumulator.model.Accumulator;
import io.numaproj.numaflow.accumulator.model.AccumulatorFactory;
import io.numaproj.numaflow.accumulator.v1.AccumulatorOuterClass;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/*
 * AccumulatorSupervisorActor orchestrates the accumulator actors and handles failures.
 * It is responsible for creating and managing the accumulator actors.
 */
@Slf4j
public class AccumulatorSupervisorActor extends AbstractActor {
    private final AccumulatorFactory<? extends Accumulator> accumulatorFactory;
    private final ActorRef shutdownActor;
    private final ActorRef outputActor;
    private final Map<String, ActorRef> actorsMap = new HashMap<>();

    public AccumulatorSupervisorActor(
            AccumulatorFactory<? extends Accumulator> accumulatorFactory,
            ActorRef shutdownActor,
            ActorRef outputActor) {
        this.accumulatorFactory = accumulatorFactory;
        this.shutdownActor = shutdownActor;
        this.outputActor = outputActor;
    }

    public static Props props(
            AccumulatorFactory<? extends Accumulator> accumulatorFactory,
            ActorRef shutdownActor,
            ActorRef outputActor) {
        return Props.create(
                AccumulatorSupervisorActor.class,
                accumulatorFactory,
                shutdownActor,
                outputActor);
    }

    // if there is an uncaught exception stop in the supervisor actor, send a signal
    // to shut down
    @Override
    public void preRestart(Throwable reason, Optional<Object> message) {
        log.debug("supervisor pre restart was executed");
        shutdownActor.tell(reason, ActorRef.noSender());

    }

    @Override
    public void postStop() {
        log.debug("post stop of supervisor executed - {}", getSelf().toString());
        shutdownActor.tell(
                Constants.SUCCESS,
                ActorRef.noSender());
    }

    @Override
    public AbstractActor.Receive createReceive() {
        return ReceiveBuilder
                .create()
                .match(AccumulatorOuterClass.AccumulatorRequest.class, this::invokeActor)
                .match(String.class, this::sendEOF)
                .build();
    }

    /*
     * Based on the incoming event, the supervisor actor will create, append or
     * close the accumulator actor.
     */
    private void invokeActor(AccumulatorOuterClass.AccumulatorRequest request) {
        String[] keys = request
                .getOperation()
                .getKeyedWindow()
                .getKeysList()
                .toArray(new String[0]);
        String uniqueId = getUniqueIdentifier(keys);

        switch (request.getOperation().getEvent()) {
            case OPEN: {
                if (!actorsMap.containsKey(uniqueId)) {
                    Accumulator accumulatorHandler = accumulatorFactory.createAccumulator();
                    ActorRef actorRef = getContext()
                            .actorOf(AccumulatorActor.props(
                                    accumulatorHandler,
                                    this.outputActor, request.getOperation().getKeyedWindow()));
                    actorsMap.put(uniqueId, actorRef);
                }
                HandlerDatum handlerDatum = constructHandlerDatum(request);
                actorsMap.get(uniqueId).tell(handlerDatum, getSelf());
                break;
            }
            case APPEND: {
                HandlerDatum handlerDatum = constructHandlerDatum(request);
                actorsMap.get(uniqueId).tell(handlerDatum, getSelf());
                break;
            }
            case CLOSE: {
                actorsMap.get(uniqueId).tell(Constants.EOF, getSelf());
                actorsMap.remove(uniqueId);
                break;
            }
            default:
        }

    }

    /*
     * Send EOF to all the accumulator actors.
     */
    private void sendEOF(String EOF) {
        for (Map.Entry<String, ActorRef> entry : actorsMap.entrySet()) {
            entry.getValue().tell(Constants.EOF, getSelf());
        }
    }

    /*
     * Construct the handler datum from the incoming request.
     */
    private HandlerDatum constructHandlerDatum(AccumulatorOuterClass.AccumulatorRequest request) {
        return new HandlerDatum(
                request.getPayload().getKeysList().toArray(new String[0]),
                request.getPayload().getValue().toByteArray(),
                Instant.ofEpochSecond(
                        request.getPayload().getWatermark().getSeconds(),
                        request.getPayload().getWatermark().getNanos()),
                Instant.ofEpochSecond(
                        request.getPayload().getEventTime().getSeconds(),
                        request.getPayload().getEventTime().getNanos()),
                request.getPayload().getHeadersMap(),
                request.getPayload().getId());
    }

    /*
     * Supervisor strategy to handle exceptions, stop all child actors in case of
     * any exception.
     */
    @Override
    public SupervisorStrategy supervisorStrategy() {
        // we want to stop all child actors in case of any exception
        return new AllForOneStrategy(
                DeciderBuilder
                        .match(Exception.class, e -> {
                            this.shutdownActor.tell(e, getSelf());
                            return SupervisorStrategy.stop();
                        })
                        .build());
    }

    /*
     * Get unique identifier for the accumulator actor.
     */
    private String getUniqueIdentifier(String[] keys) {
        return String.join(
                Constants.DELIMITER,
                keys);
    }
}
