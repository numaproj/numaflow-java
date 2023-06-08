package io.numaproj.numaflow.sink;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AllDeadLetters;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sink.handler.SinkHandler;
import io.numaproj.numaflow.sink.v1.Udsink;
import io.numaproj.numaflow.sink.v1.UserDefinedSinkGrpc;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

import static io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc.getMapFnMethod;
import static io.numaproj.numaflow.sink.SinkConstants.EOF;

@Slf4j
class SinkService extends UserDefinedSinkGrpc.UserDefinedSinkImplBase {
    public static final ActorSystem sinkActorSystem = ActorSystem.create("sink");

    private SinkHandler sinkHandler;

    public SinkService() {
    }

    public void setSinkHandler(SinkHandler sinkHandler) {
        this.sinkHandler = sinkHandler;
    }

    /**
     * Applies a function to each datum element in the stream.
     */
    @Override
    public StreamObserver<Udsink.DatumRequest> sinkFn(StreamObserver<Udsink.ResponseList> responseObserver) {
        if (this.sinkHandler == null) {
            return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(
                    getMapFnMethod(),
                    responseObserver);
        }

        CompletableFuture<Void> failureFuture = new CompletableFuture<>();

        // create a shutdown actor that listens to exceptions.
        ActorRef shutdownActorRef = sinkActorSystem.
                actorOf(SinkShutdownActor.props(responseObserver, failureFuture));

        // subscribe for dead letters
        sinkActorSystem.getEventStream().subscribe(shutdownActorRef, AllDeadLetters.class);

        handleFailure(failureFuture);

        /*
            supervisor actor to create and manage the sink actor which invokes the handlers.
        */
        ActorRef supervisorActor = sinkActorSystem
                .actorOf(SinkSupervisorActor.props(
                        sinkHandler,
                        shutdownActorRef,
                        responseObserver
                ));

        return new StreamObserver<Udsink.DatumRequest>() {
            @Override
            public void onNext(Udsink.DatumRequest d) {
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
    public void isReady(Empty request, StreamObserver<Udsink.ReadyResponse> responseObserver) {
        responseObserver.onNext(Udsink.ReadyResponse.newBuilder().setReady(true).build());
        responseObserver.onCompleted();
    }

    // wrap the exception and let it be handled in the central error handling logic.
    private void handleFailure(CompletableFuture<Void> failureFuture) {
        new Thread(() -> {
            try {
                failureFuture.get();
            } catch (Exception e) {
                throw new RuntimeException("error in sinker fn", e);
            }
        }).start();
    }
}
