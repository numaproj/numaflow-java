package io.numaproj.numaflow.sessionreducer;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ChildRestartStats;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import akka.japi.pf.ReceiveBuilder;
import com.google.common.base.Preconditions;
import com.google.protobuf.Timestamp;
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
    private final ActorRef outputActor;
    // actorMap maintains a map of active sessions.
    // key is the unique id of a session, value is the reference to the actor working on the session.
    private final Map<String, ActorRef> actorsMap = new HashMap<>();
    // mergeTracker keeps track of the merge tasks that are in progress.
    // key is the unique id of a merged task, value is how many accumulators are pending aggregation for this task.
    private final Map<String, Integer> mergeTracker = new HashMap<>();

    public SupervisorActor(
            SessionReducerFactory<? extends SessionReducer> sessionReducerFactory,
            ActorRef shutdownActor,
            ActorRef outputActor) {
        this.sessionReducerFactory = sessionReducerFactory;
        this.shutdownActor = shutdownActor;
        this.outputActor = outputActor;
    }

    public static Props props(
            SessionReducerFactory<? extends SessionReducer> sessionReducerFactory,
            ActorRef shutdownActor,
            ActorRef outputActor) {
        return Props.create(
                SupervisorActor.class,
                sessionReducerFactory,
                shutdownActor,
                outputActor);
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
                .match(GetAccumulatorResponse.class, this::handleGetAccumulatorResponse)
                .build();
    }

    private void handleEOF(String EOF) {
        // At Go sdk side, the server relies on the CLOSE request to close a session.
        // To reflect the same behaviour, we define handleEOF as a no-op.

        // TODO - should close all. - only when EOF is received can we shutdown the system by
        // setting actorResponse isLast to true. - this can be achieved by add a isInputStreamClosed attribute to the supervisor.
    }

    private void handleReduceRequest(Sessionreduce.SessionReduceRequest request) {
        Sessionreduce.SessionReduceRequest.WindowOperation windowOperation = request.getOperation();
        switch (windowOperation.getEvent()) {
            case OPEN: {
                log.info("supervisor received an open request\n");
                if (windowOperation.getKeyedWindowsCount() != 1) {
                    throw new RuntimeException(
                            "open operation error: expected exactly one window");
                }
                ActorRequest createRequest = new ActorRequest(
                        ActorRequestType.OPEN,
                        windowOperation.getKeyedWindows(0),
                        null,
                        request.hasPayload() ? request.getPayload():null,
                        "");
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
                        request.hasPayload() ? request.getPayload():null,
                        "");
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
                                    null,
                                    ""
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
                // we divide the EXPAND request to two. One is to update the actor.
                // The other is to send the payload to the updated actor.
                // because in AKKA's actor model, message processing within a single actor is sequential,
                // we ensure that the payload is handled using the updated keyed window.

                // 1. ask the session reducer actor to update its keyed window.
                // update the map to use the new id to point to the actor.
                ActorRequest expandRequest = new ActorRequest(
                        ActorRequestType.EXPAND,
                        windowOperation.getKeyedWindows(0),
                        windowOperation.getKeyedWindows(1),
                        // do not send payload
                        null,
                        "");
                this.invokeActor(expandRequest);
                this.actorsMap.put(newId, this.actorsMap.get(currentId));
                this.actorsMap.remove(currentId);

                // 2. send the payload to the updated actor.
                ActorRequest appendRequest = new ActorRequest(
                        ActorRequestType.APPEND,
                        windowOperation.getKeyedWindows(1),
                        null,
                        request.hasPayload() ? request.getPayload():null,
                        ""
                );
                this.invokeActor(appendRequest);
                break;
            }
            case MERGE: {
                Timestamp mergedStartTime = windowOperation.getKeyedWindows(0).getStart();
                Timestamp mergedEndTime = windowOperation.getKeyedWindows(0).getEnd();
                for (Sessionreduce.KeyedWindow window : windowOperation.getKeyedWindowsList()) {
                    String id = UniqueIdGenerator.getUniqueIdentifier(window);
                    if (!this.actorsMap.containsKey(id)) {
                        throw new RuntimeException(
                                "merge operation error: session not found for id: " + id);
                    }
                    // mergedWindow will be the largest window which contains all the windows.
                    if (Instant
                            .ofEpochSecond(
                                    window.getStart().getSeconds(),
                                    window.getStart().getNanos())
                            .isBefore(Instant.ofEpochSecond(
                                    mergedStartTime.getSeconds(),
                                    mergedStartTime.getNanos()))) {
                        mergedStartTime = window.getStart();
                    }
                    if (Instant
                            .ofEpochSecond(
                                    window.getEnd().getSeconds(),
                                    window.getEnd().getNanos())
                            .isAfter(Instant.ofEpochSecond(
                                    mergedEndTime.getSeconds(),
                                    mergedEndTime.getNanos()))) {
                        mergedEndTime = window.getEnd();
                    }
                }

                Sessionreduce.KeyedWindow mergedWindow = Sessionreduce.KeyedWindow.newBuilder().
                        setStart(mergedStartTime)
                        .setEnd(mergedEndTime)
                        .addAllKeys(windowOperation.getKeyedWindows(0).getKeysList())
                        .setSlot(windowOperation.getKeyedWindows(0).getSlot()).build();
                // open a new session for the merged keyed window.
                ActorRequest mergeOpenRequest = new ActorRequest(
                        // TODO - is it possible that all window merged to the first window, hence the window already exists - handle this case.
                        // if the window already exists, let's still create a new actor ref and change the reference. the existing one will close itself when returning accumulator.
                        // wait, after we create the new ref, we lose the access to the existing ref?
                        // hence we will never get the accumulator...
                        // TODO - need to unit test it.
                        ActorRequestType.MERGE_OPEN,
                        mergedWindow,
                        null,
                        null,
                        "");
                this.invokeActor(mergeOpenRequest);

                String mergeTaskId = UniqueIdGenerator.getUniqueIdentifier(mergedWindow);
                this.mergeTracker.put(mergeTaskId, windowOperation.getKeyedWindowsCount());


                for (Sessionreduce.KeyedWindow window : windowOperation.getKeyedWindowsList()) {
                    // tell the session reducer actor - "hey, you are about to be merged."
                    ActorRequest getAccumulatorRequest = new ActorRequest(
                            ActorRequestType.GET_ACCUMULATOR,
                            window,
                            null,
                            null,
                            mergeTaskId
                    );
                    this.invokeActor(getAccumulatorRequest);
                }
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
                                this.outputActor,
                                false));
                log.info("putting id: " + uniqueId);
                this.actorsMap.put(uniqueId, actorRef);
                System.out.println("putted in the map id: " + uniqueId);
                break;
            }
            case APPEND: {
                if (!this.actorsMap.containsKey(uniqueId)) {
                    log.info(
                            "supervisor received an APPEND request, but actor doesn't exist, creating one...\n");
                    SessionReducer sessionReducer = sessionReducerFactory.createSessionReducer();
                    ActorRef actorRef = getContext()
                            .actorOf(SessionReducerActor.props(
                                    actorRequest.getKeyedWindow(),
                                    sessionReducer,
                                    this.outputActor,
                                    false));
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
                System.out.println("I am telling the actor to expand...");
                this.actorsMap.get(uniqueId).tell(actorRequest.getNewKeyedWindow(), getSelf());
                break;
            }
            case MERGE_OPEN: {
                if (this.actorsMap.containsKey(uniqueId)) {
                    // TODO - this is not right - we need to aggregate for the existing window.
                    log.info(
                            "the merged keyed window is the same as one of the existing windows, replace.");
                }
                SessionReducer sessionReducer = sessionReducerFactory.createSessionReducer();
                ActorRef actorRef = getContext()
                        .actorOf(SessionReducerActor.props(
                                actorRequest.getKeyedWindow(),
                                sessionReducer,
                                this.outputActor,
                                true));
                log.info("putting id: " + uniqueId);
                this.actorsMap.put(uniqueId, actorRef);
                break;
            }
            case GET_ACCUMULATOR: {
                this.actorsMap
                        .get(uniqueId)
                        .tell(
                                new GetAccumulatorRequest(actorRequest.getMergeTaskId()),
                                getSelf());
                break;
            }
        }

        if (actorRequest.getPayload() != null) {
            log.info("verifying payload content of a not-set one");
            log.info(actorRequest.getPayload().toString());
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
            // TODO - FIXME - only when received EOF can we clean up the system
            // since the actors map is empty, this particular actor response is the last response to forward to output gRPC stream.
            log.info("I am cleaning up the system...");
            actorResponse.setLast(true);
            this.outputActor.tell(actorResponse, getSelf());
        } else {
            this.outputActor.tell(actorResponse, getSelf());
        }
    }

    private void handleGetAccumulatorResponse(GetAccumulatorResponse getAccumulatorResponse) {
        String mergeTaskId = getAccumulatorResponse.getMergeTaskId();
        if (!mergeTracker.containsKey(mergeTaskId)) {
            throw new RuntimeException(
                    "received an accumulator but the corresponding merge task doesn't exist.");
        }
        this.mergeTracker.put(mergeTaskId, this.mergeTracker.get(mergeTaskId) - 1);
        // the supervisor gets the accumulator of the session and immediately releases the session.
        // the session is released without being explicitly closed because it has been merged and tracked by the newly merged session.
        // we release a session by simply removing it from the actor map so that the corresponding actor
        // gets de-referred and handled by Java GC.
        this.actorsMap.remove(UniqueIdGenerator.getUniqueIdentifier(getAccumulatorResponse.getFromKeyedWindow()));
        this.actorsMap.get(mergeTaskId).tell(new MergeAccumulatorRequest(
                this.mergeTracker.get(mergeTaskId) == 0,
                getAccumulatorResponse.getAccumulator()), getSelf());
        if (this.mergeTracker.get(mergeTaskId) == 0) {
            // remove the task from the merge tracker when there is no more pending accumulators to merge.
            this.mergeTracker.remove(mergeTaskId);
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
