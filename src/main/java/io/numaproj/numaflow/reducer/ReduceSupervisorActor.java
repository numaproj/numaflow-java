package io.numaproj.numaflow.reducer;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ChildRestartStats;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import akka.japi.pf.ReceiveBuilder;
import com.google.common.base.Preconditions;
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import lombok.extern.slf4j.Slf4j;
import scala.PartialFunction;
import scala.collection.Iterable;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

/**
 * ReduceSupervisorActor actor distributes the messages to actors and handles failure.
 */
@Slf4j
class ReduceSupervisorActor extends AbstractActor {
    private final ReducerFactory<? extends Reducer> reducerFactory;
    private final Metadata md;
    private final ActorRef shutdownActor;
    private final StreamObserver<ReduceOuterClass.ReduceResponse> responseObserver;
    private final Map<String, ActorInfo> windowMap = new HashMap<>();

    // Inner class to hold actor information
    private static class ActorInfo {
        final Map<String, ActorRef> actorsMap;
        final ReduceOuterClass.Window window;

        ActorInfo(ReduceOuterClass.Window window) {
            this.actorsMap = new HashMap<>();
            this.window = window;
        }
    }

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
                .match(ActorRequest.class, this::invokeActors)
                .match(String.class, this::sendEOF)
                .match(ActorResponse.class, this::responseListener)
                .build();
    }

    /*
        based on the keys of the input message invoke the right actor
        if there is no actor for an incoming set of keys, create a new actor
        track all the child actors using actors map
     */
    private void invokeActors(ActorRequest actorRequest) {
        ReduceOuterClass.ReduceRequest request = actorRequest.getRequest();
        ReduceOuterClass.ReduceRequest.WindowOperation operation = request.getOperation();
        ReduceOuterClass.ReduceRequest.WindowOperation.Event event = operation.getEvent();
        
        switch (event) {
            case OPEN:
                // For OPEN, create a new actor for the window and keys
                String[] keys = actorRequest.getKeySet();
                String uniqueId = actorRequest.getUniqueIdentifier();
                
                // Create a new reducer actor
                Reducer reduceHandler = reducerFactory.createReducer();
                ActorRef actorRef = getContext()
                        .actorOf(ReduceActor.props(keys, md, reduceHandler));
                
                // Track this actor by its window
                String windowId = getWindowId(operation.getWindows(0));
                windowMap.computeIfAbsent(windowId, k -> new ActorInfo(operation.getWindows(0)))
                       .actorsMap.put(uniqueId, actorRef);

                // Process the payload if present
                if (request.hasPayload()) {
                    HandlerDatum handlerDatum = constructHandlerDatum(request.getPayload());
                    actorRef.tell(handlerDatum, getSelf());
                }
                break;
                
            case APPEND:
                // For APPEND, use existing actor
                String appendUniqueId = actorRequest.getUniqueIdentifier();
                String appendWindowId = getWindowId(operation.getWindows(0));
                
                ActorInfo actorInfo = windowMap.get(appendWindowId);
                if (actorInfo == null || !actorInfo.actorsMap.containsKey(appendUniqueId)) {
                    log.warn("Received APPEND for non-existent actor: {}", appendUniqueId);
                    break;
                }
                
                // Process the payload
                if (request.hasPayload()) {
                    HandlerDatum appendHandlerDatum = constructHandlerDatum(request.getPayload());
                    actorInfo.actorsMap.get(appendUniqueId).tell(appendHandlerDatum, getSelf());
                }
                break;
                
            case CLOSE:
                // For CLOSE, we need to find all actors with matching window
                String closeWindowId = getWindowId(operation.getWindows(0));
                ActorInfo closeActorInfo = windowMap.get(closeWindowId);
                
                if (closeActorInfo != null) {
                    // Send EOF to all actors for this window
                    for (Map.Entry<String, ActorRef> entry : closeActorInfo.actorsMap.entrySet()) {
                        entry.getValue().tell(Constants.EOF, getSelf());
                    }
                }
                break;
                
            default:
                log.warn("Unsupported operation: {}", event);
        }
    }

    private void sendEOF(String EOF) {
        for (ActorInfo actorInfo : windowMap.values()) {
            for (ActorRef actor : actorInfo.actorsMap.values()) {
                actor.tell(EOF, getSelf());
            }
        }
    }

    // listen to child actors for the result.
    private void responseListener(ActorResponse actorResponse) {
        ReduceOuterClass.Window window = actorResponse.getResponse().getWindow();
        String windowId = getWindowId(window);
        String actorId = actorResponse.getUniqueIdentifier();
        
        ActorInfo actorInfo = windowMap.get(windowId);
        if (actorInfo == null) {
            log.warn("Received response for unknown window: {}", windowId);
            return;
        }
        
        if (actorResponse.getResponse().getEOF()) {
            actorInfo.actorsMap.remove(actorId);
            
            // If this window has no more actors, remove it
            if (actorInfo.actorsMap.isEmpty()) {
                responseObserver.onNext(actorResponse.getResponse());
                windowMap.remove(windowId);
            }
            
            // If all windows are processed, send EOF and complete
            if (windowMap.isEmpty()) {
                responseObserver.onCompleted();
                getContext().getSystem().stop(getSelf());
            }
        } else {
            // send non-EOF responses to the output stream.
            responseObserver.onNext(actorResponse.getResponse());
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

    // Helper method to get a unique ID for a window
    private String getWindowId(ReduceOuterClass.Window window) {
        long startMillis = convertToEpochMilli(window.getStart());
        long endMillis = convertToEpochMilli(window.getEnd());
        return String.format(
                "%d:%d",
                startMillis, endMillis);
    }

    private long convertToEpochMilli(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()).toEpochMilli();
    }
}
