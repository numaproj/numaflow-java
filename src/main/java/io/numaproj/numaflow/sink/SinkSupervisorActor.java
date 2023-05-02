package io.numaproj.numaflow.sink;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ChildRestartStats;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import akka.japi.pf.ReceiveBuilder;
import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sink.handler.SinkHandler;
import io.numaproj.numaflow.sink.types.ResponseList;
import io.numaproj.numaflow.sink.v1.Udsink;
import lombok.extern.slf4j.Slf4j;
import scala.PartialFunction;
import scala.collection.Iterable;

import java.time.Instant;
import java.util.Optional;

/**
 * Supervisor actor to supervise and manage sink actor which invokes the sink handler
 */

@Slf4j
class SinkSupervisorActor extends AbstractActor {
    private final SinkHandler sinkHandler;
    private final ActorRef shutdownActor;
    private ActorRef sinkActor;
    private final StreamObserver<Udsink.ResponseList> responseObserver;

    public SinkSupervisorActor(
            SinkHandler sinkHandler,
            ActorRef shutdownActor,
            StreamObserver<Udsink.ResponseList> responseObserver) {
        this.sinkHandler = sinkHandler;
        this.shutdownActor = shutdownActor;
        this.responseObserver = responseObserver;
    }

    public static Props props(
            SinkHandler sinkHandler,
            ActorRef shutdownActor,
            StreamObserver<Udsink.ResponseList> streamObserver) {
        return Props.create(
                SinkSupervisorActor.class,
                sinkHandler,
                shutdownActor,
                streamObserver);
    }

    // if there is an uncaught exception stop in the supervisor actor, send a signal to shut down
    @Override
    public void preRestart(Throwable reason, Optional<Object> message) {
        log.debug("supervisor pre restart was executed");
        shutdownActor.tell(reason, ActorRef.noSender());
        SinkService.sinkActorSystem.stop(getSelf());
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return new SinkSupervisorStrategy();
    }


    @Override
    public void postStop() {
        log.debug("post stop of supervisor executed - {}", getSelf().toString());
        shutdownActor.tell(SinkConstants.SUCCESS, ActorRef.noSender());
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder
                .create()
                .match(Udsink.DatumRequest.class, this::invokeActor)
                .match(ResponseList.class, this::processResponse)
                .match(String.class, this::sendEOF)
                .build();
    }

    private void invokeActor(Udsink.DatumRequest datumRequest) {
        if (sinkActor == null) {
            sinkActor = getContext().actorOf(SinkActor.props(sinkHandler));
        }
        sinkActor.tell(constructHandlerDatum(datumRequest), getSelf());
    }

    private void processResponse(ResponseList responseList) {
        responseObserver.onNext(buildResponseList(responseList));
        responseObserver.onCompleted();
        // after the stream is processed stop the actor.
        getContext().getSystem().stop(getSelf());
    }

    private void sendEOF(String EOF) {
        sinkActor.tell(EOF, getSelf());
    }


    /*
        We need supervisor to handle failures, by default if there are any failures
        actors will be restarted, but we want to escalate the exception and terminate
        the system.
    */
    private final class SinkSupervisorStrategy extends SupervisorStrategy {

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
            getContext().getSystem().stop(getSelf());
        }
    }

    private HandlerDatum constructHandlerDatum(Udsink.DatumRequest d) {
        return new HandlerDatum(
                d.getKeysList().toArray(new String[0]),
                d.getValue().toByteArray(),
                Instant.ofEpochSecond(
                        d.getWatermark().getWatermark().getSeconds(),
                        d.getWatermark().getWatermark().getNanos()),
                Instant.ofEpochSecond(
                        d.getEventTime().getEventTime().getSeconds(),
                        d.getEventTime().getEventTime().getNanos()),
                d.getId(),
                false);
    }

    public Udsink.ResponseList buildResponseList(ResponseList responses) {
        var responseListBuilder = Udsink.ResponseList.newBuilder();
        responses.getResponses().forEach(response -> {
            responseListBuilder.addResponses(Udsink.Response.newBuilder()
                    .setId(response.getId() == null ? "" : response.getId())
                    .setErrMsg(response.getErr() == null ? "" : response.getErr())
                    .setSuccess(response.getSuccess())
                    .build());
        });
        return responseListBuilder.build();
    }
}
