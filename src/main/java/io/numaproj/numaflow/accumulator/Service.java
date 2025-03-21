package io.numaproj.numaflow.accumulator;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AllDeadLetters;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.google.rpc.Code;
import com.google.rpc.DebugInfo;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.accumulator.model.Accumulator;
import io.numaproj.numaflow.accumulator.model.AccumulatorFactory;
import io.numaproj.numaflow.accumulator.v1.AccumulatorGrpc;
import io.numaproj.numaflow.accumulator.v1.AccumulatorOuterClass;
import io.numaproj.numaflow.shared.ExceptionUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

/*
 * Grpc service for the accumulator.
 */
@Slf4j
class Service extends AccumulatorGrpc.AccumulatorImplBase {
    public static final ActorSystem accumulatorActorSystem = ActorSystem.create("accumulator");

    private final AccumulatorFactory<? extends Accumulator> accumulatorFactory;

    public Service(AccumulatorFactory<? extends Accumulator> accumulatorFactory) {
        this.accumulatorFactory = accumulatorFactory;
    }

    static void handleFailure(
            CompletableFuture<Void> failureFuture,
            StreamObserver<AccumulatorOuterClass.AccumulatorResponse> responseObserver) {
        new Thread(() -> {
            try {
                failureFuture.get();
            } catch (Exception e) {
                e.printStackTrace();
                // Build gRPC Status
                com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                        .setCode(Code.INTERNAL.getNumber())
                        .setMessage(ExceptionUtils.getExceptionErrorString() + ": " + (
                                e.getMessage() != null ? e.getMessage() : ""))
                        .addDetails(Any.pack(DebugInfo.newBuilder()
                                .setDetail(ExceptionUtils.getStackTrace(e))
                                .build()))
                        .build();
                responseObserver.onError(StatusProto.toStatusRuntimeException(status));
            }
        }).start();
    }

    /**
     * Streams input data to accumulator and returns the result.
     */
    @Override
    public StreamObserver<AccumulatorOuterClass.AccumulatorRequest> accumulateFn(
            final StreamObserver<AccumulatorOuterClass.AccumulatorResponse> responseObserver) {

        CompletableFuture<Void> failureFuture = new CompletableFuture<>();

        // create a shutdown actor that listens to exceptions.
        ActorRef shutdownActorRef = accumulatorActorSystem.actorOf(ShutdownActor.props(failureFuture));

        // subscribe for dead letters
        accumulatorActorSystem.getEventStream().subscribe(shutdownActorRef, AllDeadLetters.class);

        handleFailure(failureFuture, responseObserver);

        // create an output actor that ensures synchronized delivery of accumulator responses.
        ActorRef outputActor = accumulatorActorSystem.actorOf(OutputActor.props(responseObserver));

        /*
         * create a supervisor actor which assign the tasks to child actors.
         * we create a child actor for every unique set of keys in a window.
         */
        ActorRef supervisorActor = accumulatorActorSystem
                .actorOf(AccumulatorSupervisorActor.props(
                        accumulatorFactory,
                        shutdownActorRef,
                        outputActor));

        return new StreamObserver<>() {
            @Override
            public void onNext(AccumulatorOuterClass.AccumulatorRequest request) {
                // send the message to parent actor, which takes care of distribution.
                if (!supervisorActor.isTerminated()) {
                    supervisorActor.tell(request, ActorRef.noSender());
                } else {
                    responseObserver.onError(new Throwable("Supervisor actor was terminated"));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("Error from the client - {}", throwable.getMessage());
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                // indicate the end of input to the supervisor
                supervisorActor.tell(Constants.EOF, ActorRef.noSender());
            }
        };
    }

    /**
     * IsReady is the heartbeat endpoint for gRPC.
     */
    @Override
    public void isReady(
            Empty request,
            StreamObserver<AccumulatorOuterClass.ReadyResponse> responseObserver) {
        responseObserver.onNext(AccumulatorOuterClass.ReadyResponse
                .newBuilder()
                .setReady(true)
                .build());
        responseObserver.onCompleted();
    }
}
