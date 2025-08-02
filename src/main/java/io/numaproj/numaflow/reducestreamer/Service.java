package io.numaproj.numaflow.reducestreamer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AllDeadLetters;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.google.rpc.Code;
import com.google.rpc.DebugInfo;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.reduce.v1.ReduceGrpc;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import io.numaproj.numaflow.reducestreamer.model.IntervalWindow;
import io.numaproj.numaflow.reducestreamer.model.Metadata;
import io.numaproj.numaflow.reducestreamer.model.ReduceStreamer;
import io.numaproj.numaflow.reducestreamer.model.ReduceStreamerFactory;
import io.numaproj.numaflow.shared.ExceptionUtils;
import io.numaproj.numaflow.shared.GrpcServerUtils;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static io.numaproj.numaflow.reduce.v1.ReduceGrpc.getReduceFnMethod;

@Slf4j
class Service extends ReduceGrpc.ReduceImplBase {
    public static final ActorSystem reduceActorSystem = ActorSystem.create("reducestream");

    private final ReduceStreamerFactory<? extends ReduceStreamer> reduceStreamerFactory;

    public Service(ReduceStreamerFactory<? extends ReduceStreamer> reduceStreamerFactory) {
        this.reduceStreamerFactory = reduceStreamerFactory;
    }

    static void handleFailure(
            CompletableFuture<Void> failureFuture,
            StreamObserver<ReduceOuterClass.ReduceResponse> responseObserver) {
        new Thread(() -> {
            try {
                failureFuture.get();
            } catch (Exception e) {
                e.printStackTrace();
                com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                        .setCode(Code.INTERNAL.getNumber())
                        .setMessage(
                                ExceptionUtils.getExceptionErrorString() + ": " + (e.getMessage() != null ? e.getMessage() : ""))
                        .addDetails(Any.pack(DebugInfo.newBuilder()
                                .setDetail(ExceptionUtils.getStackTrace(e))
                                .build()))
                        .build();
                responseObserver.onError(StatusProto.toStatusRuntimeException(status));
            }
        }).start();
    }

    /**
     * Streams input data to reduceFn and returns the result.
     */
    @Override
    public StreamObserver<ReduceOuterClass.ReduceRequest> reduceFn(final StreamObserver<ReduceOuterClass.ReduceResponse> responseObserver) {
        if (this.reduceStreamerFactory == null) {
            return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(
                    getReduceFnMethod(),
                    responseObserver);
        }

        CompletableFuture<Void> failureFuture = new CompletableFuture<>();

        // create a shutdown actor that listens to exceptions.
        ActorRef shutdownActorRef = reduceActorSystem.
                actorOf(ShutdownActor.props(failureFuture));

        // subscribe for dead letters
        reduceActorSystem.getEventStream().subscribe(shutdownActorRef, AllDeadLetters.class);

        handleFailure(failureFuture, responseObserver);

        // create an output actor that ensures synchronized delivery of reduce responses.
        ActorRef outputActor = reduceActorSystem.
                actorOf(OutputActor.props(responseObserver));
        /*
            create a supervisor actor which assign the tasks to child actors.
            we create a child actor for every unique set of keys in a window.
        */
        ActorRef supervisorActor = reduceActorSystem
                .actorOf(SupervisorActor.props(
                        reduceStreamerFactory,
                        shutdownActorRef,
                        outputActor));


        return new StreamObserver<>() {
            @Override
            public void onNext(ReduceOuterClass.ReduceRequest datum) {
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
            StreamObserver<ReduceOuterClass.ReadyResponse> responseObserver) {
        responseObserver.onNext(ReduceOuterClass.ReadyResponse.newBuilder().setReady(true).build());
        responseObserver.onCompleted();
    }
}
