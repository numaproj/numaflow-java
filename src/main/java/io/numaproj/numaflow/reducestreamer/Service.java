package io.numaproj.numaflow.reducestreamer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AllDeadLetters;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.reduce.v1.ReduceGrpc;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import io.numaproj.numaflow.reducestreamer.model.IntervalWindow;
import io.numaproj.numaflow.reducestreamer.model.IntervalWindowImpl;
import io.numaproj.numaflow.reducestreamer.model.Metadata;
import io.numaproj.numaflow.reducestreamer.model.MetadataImpl;
import io.numaproj.numaflow.reducestreamer.user.ReduceStreamer;
import io.numaproj.numaflow.reducestreamer.user.ReduceStreamerFactory;
import io.numaproj.numaflow.shared.GrpcServerUtils;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static io.numaproj.numaflow.reduce.v1.ReduceGrpc.getReduceFnMethod;

@Slf4j
class Service extends ReduceGrpc.ReduceImplBase {

    public static final ActorSystem reduceActorSystem = ActorSystem.create("reducestream");

    private ReduceStreamerFactory<? extends ReduceStreamer> reduceStreamerFactory;

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
                var status = Status.UNKNOWN.withDescription(e.getMessage()).withCause(e);
                responseObserver.onError(status.asException());
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

        // get window start and end time from gPRC metadata
        String winSt = GrpcServerUtils.WINDOW_START_TIME.get();
        String winEt = GrpcServerUtils.WINDOW_END_TIME.get();

        // convert the start and end time to Instant
        Instant startTime = Instant.ofEpochMilli(Long.parseLong(winSt));
        Instant endTime = Instant.ofEpochMilli(Long.parseLong(winEt));

        // create metadata
        IntervalWindow iw = new IntervalWindowImpl(startTime, endTime);
        Metadata md = new MetadataImpl(iw);

        CompletableFuture<Void> failureFuture = new CompletableFuture<>();

        // create a shutdown actor that listens to exceptions.
        ActorRef shutdownActorRef = reduceActorSystem.
                actorOf(ReduceShutdownActor.props(failureFuture));

        // subscribe for dead letters
        reduceActorSystem.getEventStream().subscribe(shutdownActorRef, AllDeadLetters.class);

        handleFailure(failureFuture, responseObserver);

        // create a response stream actor that ensures synchronized delivery of reduce responses.
        ActorRef responseStreamActor = reduceActorSystem.
                actorOf(ResponseStreamActor.props(responseObserver, md));
        /*
            create a supervisor actor which assign the tasks to child actors.
            we create a child actor for every unique set of keys in a window.
        */
        ActorRef supervisorActor = reduceActorSystem
                .actorOf(ReduceSupervisorActor.props(
                        reduceStreamerFactory,
                        md,
                        shutdownActorRef,
                        responseStreamActor,
                        responseObserver));


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
