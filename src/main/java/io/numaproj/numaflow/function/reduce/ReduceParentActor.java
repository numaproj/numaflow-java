package io.numaproj.numaflow.function.reduce;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ChildRestartStats;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.Patterns;
import com.google.common.base.Preconditions;
import io.numaproj.numaflow.function.FunctionService;
import io.numaproj.numaflow.function.HandlerDatum;
import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.v1.Udfunction;
import lombok.extern.slf4j.Slf4j;
import scala.PartialFunction;
import scala.collection.Iterable;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class ReduceParentActor extends AbstractActor {
    private final Class<? extends GroupBy> groupBy;
    private final Metadata md;
    private final Map<String, ActorRef> actorsMap = new HashMap<>();
    private final List<Future<Object>> results = new ArrayList<>();

    public ReduceParentActor(
            Class<? extends GroupBy> groupBy,
            Metadata md) {
        this.groupBy = groupBy;
        this.md = md;
    }

    public static Props props(Class<? extends GroupBy> groupBy, Metadata md) {
        return Props.create(ReduceParentActor.class, groupBy, md);
    }

    // if there is an uncaught exception inform the sender and terminate the system
    @Override
    public void preRestart(Throwable reason, Optional<Object> message) {
        getSender().tell(reason, getSelf());
        getContext().getSystem().terminate();
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return new ReduceSupervisorStratergy();
    }

    private final class ReduceSupervisorStratergy extends SupervisorStrategy {

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

            Preconditions.checkArgument(!restart, "on failures, we will never restart our actors");
            /*
                   indicate the sender about the exception.
                   stop the parent and all the child actors will automatically be terminated.
             */

            getSender().tell(cause, getSelf());
            FunctionService.actorSystem.stop(getSelf());
        }
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder
                .create()
                .match(Udfunction.Datum.class, this::invokeActors)
                .match(String.class, this::sendEOF)
                .build();
    }

    private void invokeActors(Udfunction.Datum  datum) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (!actorsMap.containsKey(datum.getKey())) {

            GroupBy g = groupBy
                    .getDeclaredConstructor(String.class, Metadata.class)
                    .newInstance(datum.getKey(), md);


            ActorRef actorRef = getContext()
                    .actorOf(ReduceActor.props(g));

            actorsMap.put(datum.getKey(), actorRef);
        }
        HandlerDatum handlerDatum = constructHandlerDatum(datum);
        actorsMap.get(datum.getKey()).tell(handlerDatum, getSelf());
    }

    private void sendEOF(String EOF) {
        for(Map.Entry<String, ActorRef> entry: actorsMap.entrySet()) {
            results.add(Patterns.ask(entry.getValue(), EOF, Integer.MAX_VALUE));
        }

        getSender().tell(results, getSelf());
    }

    private HandlerDatum constructHandlerDatum(Udfunction.Datum datum) {
        return new HandlerDatum(
                datum.getValue().toByteArray(),
                Instant.ofEpochSecond(
                        datum.getWatermark().getWatermark().getSeconds(),
                        datum.getWatermark().getWatermark().getNanos()),
                Instant.ofEpochSecond(
                        datum.getEventTime().getEventTime().getSeconds(),
                        datum.getEventTime().getEventTime().getNanos()));
    }
}
