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
    // number of accumulators to be collected by current MERGE request.
    private int numberOfPendingAccumulators;
    // when set to true, isInputStreamClosed means the gRPC input stream has reached EOF.
    private boolean isInputStreamClosed = false;
    // mergeRequestSender is used to track the reference to the merge request sender,
    // it's used to report back when the MERGE request is completed.
    // the mergeRequest is sent using ask, which creates a temporary actor as sender behind the scene.
    private ActorRef mergeRequestSender;

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
                .build();
    }

    private void handleEOF(String EOF) {
        this.isInputStreamClosed = true;
        if (actorsMap.isEmpty()) {
            this.outputActor.tell(EOF, getSelf());
            return;
        }
        for (Map.Entry<String, ActorRef> entry : actorsMap.entrySet()) {
            entry.getValue().tell(EOF, getSelf());
        }
    }

    private void handleReduceRequest(Sessionreduce.SessionReduceRequest request) {
        Sessionreduce.SessionReduceRequest.WindowOperation windowOperation = request.getOperation();
        switch (windowOperation.getEvent()) {
            case OPEN: {
                if (windowOperation.getKeyedWindowsCount() != 1) {
                    throw new RuntimeException(
                            "open operation error: expected exactly one window");
                }
                String windowId = UniqueIdGenerator.getUniqueIdentifier(windowOperation.getKeyedWindows(
                        0));
                if (this.actorsMap.containsKey(windowId)) {
                    throw new RuntimeException(
                            "received an OPEN request but the session reducer actor already exists");
                }

                ActorRequest createRequest = ActorRequest.builder()
                        .type(ActorRequestType.OPEN)
                        .keyedWindow(windowOperation.getKeyedWindows(0))
                        .payload(request.hasPayload() ? request.getPayload() : null)
                        .build();
                this.invokeActor(createRequest);
                break;
            }
            case APPEND: {
                if (windowOperation.getKeyedWindowsCount() != 1) {
                    throw new RuntimeException(
                            "append operation error: expected exactly one window");
                }
                ActorRequest appendRequest = ActorRequest.builder()
                        .type(ActorRequestType.APPEND)
                        .keyedWindow(windowOperation.getKeyedWindows(0))
                        .payload(request.hasPayload() ? request.getPayload() : null)
                        .build();
                this.invokeActor(appendRequest);
                break;
            }
            case CLOSE: {
                windowOperation.getKeyedWindowsList().forEach(
                        keyedWindow -> {
                            ActorRequest closeRequest = ActorRequest.builder()
                                    .type(ActorRequestType.CLOSE)
                                    .keyedWindow(keyedWindow)
                                    .build();
                            this.invokeActor(closeRequest);
                        });
                break;
            }
            case EXPAND: {
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
                ActorRequest expandRequest = ActorRequest.builder()
                        .type(ActorRequestType.EXPAND)
                        .keyedWindow(windowOperation.getKeyedWindows(0))
                        .newKeyedWindow(windowOperation.getKeyedWindows(1))
                        .build();
                this.invokeActor(expandRequest);
                this.actorsMap.put(newId, this.actorsMap.get(currentId));
                this.actorsMap.remove(currentId);

                // 2. send the payload to the updated actor.
                ActorRequest appendRequest = ActorRequest.builder()
                        .type(ActorRequestType.APPEND)
                        .keyedWindow(windowOperation.getKeyedWindows(1))
                        .payload(request.hasPayload() ? request.getPayload() : null)
                        .build();
                this.invokeActor(appendRequest);
                break;
            }
            case MERGE: {
                this.mergeRequestSender = getSender();
                Timestamp mergedStartTime = windowOperation.getKeyedWindows(0).getStart();
                Timestamp mergedEndTime = windowOperation.getKeyedWindows(0).getEnd();
                for (Sessionreduce.KeyedWindow window : windowOperation.getKeyedWindowsList()) {
                    String id = UniqueIdGenerator.getUniqueIdentifier(window);
                    if (!this.actorsMap.containsKey(id)) {
                        throw new RuntimeException(
                                "merge operation error: session not found for id: " + id);
                    }
                    // merged window will be the largest window which contains all the windows.
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

                String mergeTaskId = UniqueIdGenerator.getUniqueIdentifier(mergedWindow);
                this.numberOfPendingAccumulators = windowOperation.getKeyedWindowsCount();
                for (Sessionreduce.KeyedWindow window : windowOperation.getKeyedWindowsList()) {
                    // tell the session reducer actor - "hey, you are about to be merged."
                    ActorRequest getAccumulatorRequest = ActorRequest.builder()
                            .type(ActorRequestType.GET_ACCUMULATOR)
                            .keyedWindow(window)
                            .mergeTaskId(mergeTaskId)
                            .build();
                    this.invokeActor(getAccumulatorRequest);
                }
                // open a new session for the merged keyed window.
                // it's possible that merged keyed window is the same as one of the existing windows,
                // in this case, since we already send out the GET_ACCUMULATOR request, it's ok to replace
                // the existing window with the new one.
                // the accumulator of the old window will get merged to the new window eventually,
                // when the supervisor receives the get accumulator response.
                ActorRequest openRequest = ActorRequest.builder()
                        .type(ActorRequestType.OPEN)
                        .keyedWindow(mergedWindow)
                        .build();
                this.invokeActor(openRequest);
                break;
            }
            default:
                throw new RuntimeException(
                        "received an unsupported window operation: " + windowOperation.getEvent());
        }
    }

    private void invokeActor(ActorRequest actorRequest) {
        String uniqueId = UniqueIdGenerator.getUniqueIdentifier(actorRequest.getKeyedWindow());
        switch (actorRequest.getType()) {
            case OPEN: {
                SessionReducer sessionReducer = sessionReducerFactory.createSessionReducer();
                ActorRef actorRef = getContext()
                        .actorOf(SessionReducerActor.props(
                                actorRequest.getKeyedWindow(),
                                sessionReducer,
                                this.outputActor
                        ));
                this.actorsMap.put(uniqueId, actorRef);
                break;
            }
            case APPEND: {
                if (!this.actorsMap.containsKey(uniqueId)) {
                    SessionReducer sessionReducer = sessionReducerFactory.createSessionReducer();
                    ActorRef actorRef = getContext()
                            .actorOf(SessionReducerActor.props(
                                    actorRequest.getKeyedWindow(),
                                    sessionReducer,
                                    this.outputActor));
                    this.actorsMap.put(uniqueId, actorRef);
                }
                break;
            }
            case CLOSE: {
                if (this.actorsMap.containsKey(uniqueId)) {
                    this.actorsMap.get(uniqueId).tell(Constants.EOF, getSelf());
                }
                break;
            }
            case EXPAND: {
                this.actorsMap.get(uniqueId).tell(actorRequest.getNewKeyedWindow(), getSelf());
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
            HandlerDatum handlerDatum = constructHandlerDatum(actorRequest.getPayload());
            this.actorsMap.get(uniqueId).tell(handlerDatum, getSelf());
        }
    }

    private void handleActorResponse(ActorResponse actorResponse) {
        String responseWindowId = UniqueIdGenerator.getUniqueIdentifier(actorResponse
                .getResponse()
                .getKeyedWindow());

        if (actorResponse.isEOFResponse()) {
            // when the supervisor receives an EOF actor response,
            // it means the corresponding session reducer actor has finished its job.
            // we remove the entry from the actors map.
            this.actorsMap.remove(responseWindowId);
            if (this.actorsMap.isEmpty() && this.isInputStreamClosed) {
                // the actor map is empty and the gRPC input stream has been closed, hence this is the very last response of the entire system.
                // this is the only place to set last.
                actorResponse.setLast(true);
                this.outputActor.tell(actorResponse, getSelf());
            } else {
                this.outputActor.tell(actorResponse, getSelf());
            }
        } else {
            // handle get accumulator response
            String mergeTaskId = actorResponse.getMergeTaskId();
            if (!this.actorsMap.containsKey(mergeTaskId)) {
                throw new RuntimeException(
                        "received an accumulator but the corresponding parent merge session doesn't exist.");
            }
            this.numberOfPendingAccumulators--;
            if (!responseWindowId.equals(mergeTaskId)) {
                // release the session that returns us the accumulator, indicating it has finished its lifecycle.
                // the session is released without being explicitly closed because it has been merged and tracked by the newly merged session.
                // we release a session by simply removing it from the actor map so that the corresponding actor
                // gets de-referred and handled by Java GC.
                this.actorsMap.remove(responseWindowId);
            }
            this.actorsMap.get(mergeTaskId).tell(
                    new MergeAccumulatorRequest(
                            actorResponse.getAccumulator()), getSelf());
            if (this.numberOfPendingAccumulators == 0) {
                // tell the gRPC input stream that the merge request is completely processed.
                this.mergeRequestSender.tell(new MergeDoneResponse(), getSelf());
            }
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
                        payload.getEventTime().getNanos()),
                payload.getHeadersMap()
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
