package io.numaproj.numaflow.sourcetransformer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sourcetransformer.v1.SourceTransformGrpc;
import io.numaproj.numaflow.sourcetransformer.v1.Sourcetransformer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

import static io.numaproj.numaflow.sourcetransformer.v1.SourceTransformGrpc.getSourceTransformFnMethod;

@Slf4j
@AllArgsConstructor
class Service extends SourceTransformGrpc.SourceTransformImplBase {

    public static final ActorSystem transformerActorSystem = ActorSystem.create("transformer");

    private final SourceTransformer transformer;

    // TODO we need to propagate the exception all the way up and shutdown the server.
    static void handleFailure(
            CompletableFuture<Void> failureFuture) {
        new Thread(() -> {
            try {
                failureFuture.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    @Override
    public StreamObserver<Sourcetransformer.SourceTransformRequest> sourceTransformFn(final StreamObserver<Sourcetransformer.SourceTransformResponse> responseObserver) {

        if (this.transformer == null) {
            return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(
                    getSourceTransformFnMethod(),
                    responseObserver);
        }

        CompletableFuture<Void> failureFuture = new CompletableFuture<>();

        handleFailure(failureFuture);

        // create a TransformSupervisorActor that processes the transform requests.
        ActorRef transformSupervisorActor = transformerActorSystem
                .actorOf(TransformSupervisorActor.props(
                        transformer,
                        responseObserver,
                        failureFuture));

        return new StreamObserver<>() {
            private boolean handshakeDone = false;

            @Override
            public void onNext(Sourcetransformer.SourceTransformRequest request) {
                // make sure the handshake is done before processing the messages
                if (!handshakeDone) {
                    if (!request.hasHandshake() || !request.getHandshake().getSot()) {
                        responseObserver.onError(Status.INVALID_ARGUMENT
                                .withDescription("Handshake request not received")
                                .asException());
                        return;
                    }
                    responseObserver.onNext(Sourcetransformer.SourceTransformResponse.newBuilder()
                            .setHandshake(request.getHandshake())
                            .build());
                    handshakeDone = true;
                    return;
                }
                // send the message to the TransformSupervisorActor.
                transformSupervisorActor.tell(request, ActorRef.noSender());
            }

            @Override
            public void onError(Throwable throwable) {
                transformSupervisorActor.tell(new Exception(throwable), ActorRef.noSender());
            }

            @Override
            public void onCompleted() {
                // indicate the end of input to the TransformSupervisorActor.
                transformSupervisorActor.tell(Constants.EOF, ActorRef.noSender());
            }
        };
    }

    /**
     * IsReady is the heartbeat endpoint for gRPC.
     */
    @Override
    public void isReady(
            Empty request,
            StreamObserver<Sourcetransformer.ReadyResponse> responseObserver) {
        responseObserver.onNext(Sourcetransformer.ReadyResponse
                .newBuilder()
                .setReady(true)
                .build());
        responseObserver.onCompleted();
    }
}
