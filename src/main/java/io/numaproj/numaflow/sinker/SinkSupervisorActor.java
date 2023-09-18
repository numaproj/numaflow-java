package io.numaproj.numaflow.sinker;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ChildRestartStats;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import akka.japi.pf.ReceiveBuilder;
import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sink.v1.SinkOuterClass;
import lombok.extern.slf4j.Slf4j;
import scala.PartialFunction;
import scala.collection.Iterable;

import java.time.Instant;
import java.util.Optional;

/**
 * SinkSupervisorActor actor to supervise and manage sink actor which invokes the sink handler
 */

@Slf4j
class SinkSupervisorActor extends AbstractActor {
    private final ActorRef shutdownActor;
    private final ActorRef sinkActor;
    private final StreamObserver<SinkOuterClass.SinkResponse> responseObserver;

    public SinkSupervisorActor(
            Sinker sinker,
            ActorRef shutdownActor,
            StreamObserver<SinkOuterClass.SinkResponse> responseObserver) {
        this.shutdownActor = shutdownActor;
        this.responseObserver = responseObserver;
        this.sinkActor = getContext().actorOf(SinkActor.props(sinker));
    }

    public static Props props(
            Sinker sinker,
            ActorRef shutdownActor,
            StreamObserver<SinkOuterClass.SinkResponse> streamObserver) {
        return Props.create(
                SinkSupervisorActor.class,
                sinker,
                shutdownActor,
                streamObserver);
    }

    // if there is an uncaught exception stop in the supervisor actor, send a signal to shut down
    @Override
    public void preRestart(Throwable reason, Optional<Object> message) {
        log.debug("supervisor pre restart was executed");
        shutdownActor.tell(reason, ActorRef.noSender());
        Service.sinkActorSystem.stop(getSelf());
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return new SinkSupervisorStrategy();
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
                .match(SinkOuterClass.SinkRequest.class, this::invokeActor)
                .match(ResponseList.class, this::processResponse)
                .match(String.class, this::sendEOF)
                .build();
    }

    private void invokeActor(SinkOuterClass.SinkRequest sinkRequest) {
        sinkActor.tell(constructHandlerDatum(sinkRequest), getSelf());
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

    private HandlerDatum constructHandlerDatum(SinkOuterClass.SinkRequest d) {
        return new HandlerDatum(
                d.getKeysList().toArray(new String[0]),
                d.getValue().toByteArray(),
                Instant.ofEpochSecond(
                        d.getWatermark().getSeconds(),
                        d.getWatermark().getNanos()),
                Instant.ofEpochSecond(
                        d.getEventTime().getSeconds(),
                        d.getEventTime().getNanos()),
                d.getId());
    }

    public SinkOuterClass.SinkResponse buildResponseList(ResponseList responses) {
        var responseBuilder = SinkOuterClass.SinkResponse.newBuilder();
        responses.getResponses().forEach(response -> {
            responseBuilder.addResults(SinkOuterClass.SinkResponse.Result.newBuilder()
                    .setId(response.getId() == null ? "" : response.getId())
                    .setErrMsg(response.getErr() == null ? "" : response.getErr())
                    .setSuccess(response.getSuccess())
                    .build());
        });
        return responseBuilder.build();
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
}
