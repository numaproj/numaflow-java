package io.numaproj.numaflow.sessionreducer;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ChildRestartStats;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import akka.japi.pf.ReceiveBuilder;
import com.google.common.base.Preconditions;
import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;
import io.numaproj.numaflow.sessionreducer.model.SessionReducer;
import io.numaproj.numaflow.sessionreducer.model.SessionReducerFactory;
import lombok.extern.slf4j.Slf4j;
import scala.PartialFunction;
import scala.collection.Iterable;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Supervisor actor distributes the messages to other actors and handles failures.
 */
@Slf4j
class SupervisorActor extends AbstractActor {
    private final SessionReducerFactory<? extends SessionReducer> sessionReducerFactory;
    private final ActorRef shutdownActor;
    private final ActorRef responseStreamActor;
    private final Map<String, ActorRef> actorsMap = new HashMap<>();

    // TODO - do we need this one? use @AllArgsConstructor
    public SupervisorActor(
            SessionReducerFactory<? extends SessionReducer> sessionReducerFactory,
            ActorRef shutdownActor,
            ActorRef responseStreamActor) {
        this.sessionReducerFactory = sessionReducerFactory;
        this.shutdownActor = shutdownActor;
        this.responseStreamActor = responseStreamActor;
    }

    public static Props props(
            SessionReducerFactory<? extends SessionReducer> sessionReducerFactory,
            ActorRef shutdownActor,
            ActorRef responseStreamActor) {
        return Props.create(
                SupervisorActor.class,
                sessionReducerFactory,
                shutdownActor,
                responseStreamActor);
    }

    // if there is an uncaught exception stop in the supervisor actor, send a signal to shut down
    @Override
    public void preRestart(Throwable reason, Optional<Object> message) {
        log.debug("supervisor pre restart was executed");
        shutdownActor.tell(reason, ActorRef.noSender());
        Service.sessionReduceActorSystem.stop(getSelf());
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
                .match(String.class, this::handleEOF)
                .match(Sessionreduce.SessionReduceRequest.class, this::handleReduceRequest)
                .match(ActorResponse.class, this::handleActorResponse)
                .build();
    }

    private void handleEOF(String EOF) {
        // At Go sdk side, the server relies on the CLOSE request to close a session.
        // To reflect the same behaviour, we define handleEOF as a no-op.
    }

    private void handleReduceRequest(Sessionreduce.SessionReduceRequest request) {
        Sessionreduce.SessionReduceRequest.WindowOperation windowOperation = request.getOperation();
        switch (windowOperation.getEvent()) {
            case OPEN: {
                log.info("supervisor received an open request\n");
                if (windowOperation.getKeyedWindowsCount() != 1) {
                    // TODO - test exception scenario.
                    throw new RuntimeException(
                            "open operation error: expected exactly one window");
                }
                ActorRequest createRequest = new ActorRequest(
                        ActorRequestType.OPEN,
                        windowOperation.getKeyedWindows(0),
                        null,
                        request.getPayload());
                this.invokeActor(createRequest);
                break;
            }
            case APPEND: {
                log.info("supervisor received an append request\n");
                if (windowOperation.getKeyedWindowsCount() != 1) {
                    throw new RuntimeException(
                            "append operation error: expected exactly one window");
                }
                ActorRequest appendRequest = new ActorRequest(
                        ActorRequestType.APPEND,
                        windowOperation.getKeyedWindows(0),
                        null,
                        request.getPayload());
                this.invokeActor(appendRequest);
                break;
            }
            case CLOSE: {
                log.info("supervisor received a close request\n");
                windowOperation.getKeyedWindowsList().forEach(
                        keyedWindow -> {
                            ActorRequest closeRequest = new ActorRequest(
                                    ActorRequestType.CLOSE,
                                    keyedWindow,
                                    null,
                                    // since it's a close request, we don't expect a real payload
                                    null
                            );
                            this.invokeActor(closeRequest);
                        });
                break;
            }
            case EXPAND: {
                log.info("supervisor received an expand request\n");
                if (windowOperation.getKeyedWindowsCount() != 2) {
                    throw new RuntimeException(
                            "expand operation error: expected exactly two windows");
                }
                String currentId = UniqueIdGenerator.getUniqueIdentifier(windowOperation.getKeyedWindows(
                        0));
                String newId = UniqueIdGenerator.getUniqueIdentifier(windowOperation.getKeyedWindows(
                        1));
                if (!this.actorsMap.containsKey(currentId)) {
                    throw new RuntimeException(
                            "expand operation error: session not found for id: " + currentId);
                }
                // we divide the EXPAND request to two. One is to update the actor, the other is to send the payload to the updated actor.
                // because in AKKA's actor model, message processing within a single actor is sequential, we ensure that the payload is handled using the updated keyed window.

                // 1. ask the session reducer actor to update its keyed window.
                // update the map to use the new id to point to the actor.
                ActorRequest expandRequest = new ActorRequest(
                        ActorRequestType.EXPAND,
                        windowOperation.getKeyedWindows(0),
                        windowOperation.getKeyedWindows(1),
                        // do not send payload
                        null);
                this.invokeActor(expandRequest);
                this.actorsMap.put(newId, this.actorsMap.get(currentId));
                this.actorsMap.remove(currentId);

                // 2. send the payload to the updated actor.
                ActorRequest appendRequest = new ActorRequest(
                        ActorRequestType.APPEND,
                        windowOperation.getKeyedWindows(1),
                        null,
                        request.getPayload()
                );
                this.invokeActor(appendRequest);
                break;
            }

            default:
                throw new RuntimeException(
                        "received an unsupported window operation: " + windowOperation.getEvent());
        }
    }

    private void invokeActor(ActorRequest actorRequest) {
        String uniqueId = UniqueIdGenerator.getUniqueIdentifier(actorRequest.getKeyedWindow());
        switch (actorRequest.type) {
            case OPEN: {
                if (this.actorsMap.containsKey(uniqueId)) {
                    throw new RuntimeException(
                            "received an OPEN request but the session reducer actor already exists");
                }
                SessionReducer sessionReducer = sessionReducerFactory.createSessionReducer();
                ActorRef actorRef = getContext()
                        .actorOf(SessionReducerActor.props(
                                actorRequest.getKeyedWindow(),
                                sessionReducer,
                                this.responseStreamActor));
                log.info("putting id: " + uniqueId);
                this.actorsMap.put(uniqueId, actorRef);
                break;
            }
            case APPEND: {
                if (!this.actorsMap.containsKey(uniqueId)) {
                    log.info(
                            "supervisor received an append request, but actor doesn't exist, creating one...\n");
                    SessionReducer sessionReducer = sessionReducerFactory.createSessionReducer();
                    ActorRef actorRef = getContext()
                            .actorOf(SessionReducerActor.props(
                                    actorRequest.getKeyedWindow(),
                                    sessionReducer,
                                    this.responseStreamActor));
                    this.actorsMap.put(uniqueId, actorRef);
                }
                break;
            }
            case CLOSE: {
                // if I can find an active actor, I close it. otherwise, skip.
                if (this.actorsMap.containsKey(uniqueId)) {
                    this.actorsMap.get(uniqueId).tell(Constants.EOF, getSelf());
                }
                break;
            }
            case EXPAND: {
                // ask the session reducer actor to update its keyed window.
                this.actorsMap.get(uniqueId).tell(actorRequest.getNewKeyedWindow(), getSelf());
            }
        }

        if (actorRequest.getPayload() != null) {
            log.info("sending the payload to the session reducer actor...");
            HandlerDatum handlerDatum = constructHandlerDatum(actorRequest.getPayload());
            this.actorsMap.get(uniqueId).tell(handlerDatum, getSelf());
        }
    }

    private void handleActorResponse(ActorResponse actorResponse) {
        // when the supervisor receives an actor response, it means the corresponding
        // session reducer actor has finished its job.
        // we remove the entry from the actors map.
        log.info("I am removing an actor...");
        this.actorsMap.remove(UniqueIdGenerator.getUniqueIdentifier(actorResponse.getResponse()
                .getKeyedWindow()));
        if (this.actorsMap.isEmpty()) {
            // since the actors map is empty, this particular actor response is the last response to forward to output gRPC stream.
            log.info("I am cleaning up the system...");
            actorResponse.setLast(true);
            this.responseStreamActor.tell(actorResponse, getSelf());
        } else {
            this.responseStreamActor.tell(actorResponse, getSelf());
        }
    }

    private HandlerDatum constructHandlerDatum(Sessionreduce.SessionReduceRequest.Payload payload) {
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
