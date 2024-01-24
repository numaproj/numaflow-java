package io.numaproj.numaflow.sessionreducer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AllDeadLetters;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sessionreduce.v1.SessionReduceGrpc;
import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;
import io.numaproj.numaflow.sessionreducer.model.SessionReducer;
import io.numaproj.numaflow.sessionreducer.model.SessionReducerFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

import static io.numaproj.numaflow.reduce.v1.ReduceGrpc.getReduceFnMethod;

@Slf4j
class Service extends SessionReduceGrpc.SessionReduceImplBase {
    public static final ActorSystem sessionReduceActorSystem = ActorSystem.create("sessionreduce");

    private SessionReducerFactory<? extends SessionReducer> sessionReducerFactory;

    public Service(SessionReducerFactory<? extends SessionReducer> sessionReducerFactory) {
        this.sessionReducerFactory = sessionReducerFactory;
    }

    static void handleFailure(
            CompletableFuture<Void> failureFuture,
            StreamObserver<Sessionreduce.SessionReduceResponse> responseObserver) {
        new Thread(() -> {
            try {
                failureFuture.get();
            } catch (Exception e) {
                e.printStackTrace();
                var status = Status.UNKNOWN.withDescription(e.getMessage()).withCause(e);
                responseObserver.onError(status.asException());
            }
        }).start();
    }

    /**
     * Streams input data to the session reducer functions and returns the result.
     */
    @Override
    public StreamObserver<Sessionreduce.SessionReduceRequest> sessionReduceFn(final StreamObserver<Sessionreduce.SessionReduceResponse> responseObserver) {
        if (this.sessionReducerFactory == null) {
            return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(
                    getReduceFnMethod(),
                    responseObserver);
        }

        CompletableFuture<Void> failureFuture = new CompletableFuture<>();

        // create a shutdown actor that listens to exceptions.
        ActorRef shutdownActorRef = sessionReduceActorSystem.
                actorOf(ShutdownActor.props(failureFuture));

        // subscribe for dead letters
        sessionReduceActorSystem.getEventStream().subscribe(shutdownActorRef, AllDeadLetters.class);

        handleFailure(failureFuture, responseObserver);

        // create an output actor that ensures synchronized delivery of reduce responses.
        ActorRef outputActor = sessionReduceActorSystem.actorOf(OutputActor.props(responseObserver));
        /*
            create a supervisor actor which assign the tasks to child actors.
            we create a child actor for every unique set of keys in a window.
        */
        ActorRef supervisorActor = sessionReduceActorSystem
                .actorOf(SupervisorActor.props(
                        sessionReducerFactory,
                        shutdownActorRef,
                        outputActor));

        return new StreamObserver<>() {
            @Override
            public void onNext(Sessionreduce.SessionReduceRequest datum) {
                // send the message to parent actor, which takes care of distribution.
                if (!supervisorActor.isTerminated()) {
                    supervisorActor.tell(new ActorRequest(datum), ActorRef.noSender());
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
            StreamObserver<Sessionreduce.ReadyResponse> responseObserver) {
        responseObserver.onNext(Sessionreduce.ReadyResponse.newBuilder().setReady(true).build());
        responseObserver.onCompleted();
    }
}
