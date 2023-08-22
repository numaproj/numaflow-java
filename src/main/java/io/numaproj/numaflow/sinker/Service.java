package io.numaproj.numaflow.sinker;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AllDeadLetters;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sink.v1.SinkGrpc;
import io.numaproj.numaflow.sink.v1.SinkOuterClass;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

import static io.numaproj.numaflow.sink.v1.SinkGrpc.getSinkFnMethod;
import static io.numaproj.numaflow.sinker.SinkConstants.EOF;

@Slf4j
class Service extends SinkGrpc.SinkImplBase {
    public static final ActorSystem sinkActorSystem = ActorSystem.create("sink");

    private final Sinker sinker;

    public Service(Sinker sinker) {
        this.sinker = sinker;
    }

    /**
     * Applies a function to each datum element in the stream.
     */
    @Override
    public StreamObserver<SinkOuterClass.SinkRequest> sinkFn(StreamObserver<SinkOuterClass.SinkResponse> responseObserver) {
        if (this.sinker == null) {
            return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(
                    getSinkFnMethod(),
                    responseObserver);
        }

        CompletableFuture<Void> failureFuture = new CompletableFuture<>();

        // create a shutdown actor that listens to exceptions.
        ActorRef shutdownActorRef = sinkActorSystem.
                actorOf(SinkShutdownActor.props(failureFuture));

        // subscribe for dead letters
        sinkActorSystem.getEventStream().subscribe(shutdownActorRef, AllDeadLetters.class);

        handleFailure(failureFuture, responseObserver);

        /*
            supervisor actor to create and manage the sink actor which invokes the handlers.
        */
        ActorRef supervisorActor = sinkActorSystem
                .actorOf(SinkSupervisorActor.props(
                        sinker,
                        shutdownActorRef,
                        responseObserver
                ));

        return new StreamObserver<SinkOuterClass.SinkRequest>() {
            @Override
            public void onNext(SinkOuterClass.SinkRequest d) {
                supervisorActor.tell(d, ActorRef.noSender());
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("Encountered error in sinkFn - {}", throwable.getMessage());
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                supervisorActor.tell(EOF, ActorRef.noSender());
            }
        };
    }

    /**
     * IsReady is the heartbeat endpoint for gRPC.
     */
    @Override
    public void isReady(Empty request, StreamObserver<SinkOuterClass.ReadyResponse> responseObserver) {
        responseObserver.onNext(SinkOuterClass.ReadyResponse.newBuilder().setReady(true).build());
        responseObserver.onCompleted();
    }

    // handle the exception with corresponding response status code.
    private void handleFailure(CompletableFuture<Void> failureFuture, StreamObserver<SinkOuterClass.SinkResponse> responseObserver) {
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
}
