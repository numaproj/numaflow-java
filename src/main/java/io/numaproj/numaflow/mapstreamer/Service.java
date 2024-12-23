package io.numaproj.numaflow.mapstreamer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.map.v1.MapGrpc;
import io.numaproj.numaflow.map.v1.MapOuterClass;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

import static io.numaproj.numaflow.map.v1.MapGrpc.getMapFnMethod;

/**
 * Service implements the gRPC service for map stream processing.
 * It uses Akka actors to handle multiple concurrent MapRequests.
 */
@Slf4j
@AllArgsConstructor
class Service extends MapGrpc.MapImplBase {

    private static final ActorSystem mapperActorSystem = ActorSystem.create("mapstreamer");
    private final MapStreamer mapStreamer;
    private final CompletableFuture<Void> shutdownSignal;

    @Override
    public StreamObserver<MapOuterClass.MapRequest> mapFn(final StreamObserver<MapOuterClass.MapResponse> responseObserver) {
        if (this.mapStreamer == null) {
            return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(
                    getMapFnMethod(),
                    responseObserver);
        }

        ActorRef mapStreamSupervisorActor = mapperActorSystem.actorOf(
                MapStreamSupervisorActor.props(mapStreamer, responseObserver, shutdownSignal)
        );

        return new StreamObserver<>() {
            private boolean handshakeDone = false;

            @Override
            public void onNext(MapOuterClass.MapRequest request) {
                if (!handshakeDone) {
                    if (!request.hasHandshake() || !request.getHandshake().getSot()) {
                        responseObserver.onError(Status.INVALID_ARGUMENT
                                .withDescription("Handshake request not received")
                                .asException());
                        return;
                    }
                    responseObserver.onNext(MapOuterClass.MapResponse.newBuilder()
                            .setHandshake(request.getHandshake())
                            .build());
                    handshakeDone = true;
                } else {
                    mapStreamSupervisorActor.tell(request, ActorRef.noSender());
                }
            }

            @Override
            public void onError(Throwable throwable) {
                mapStreamSupervisorActor.tell(new Exception(throwable), ActorRef.noSender());
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    @Override
    public void isReady(
            Empty request,
            StreamObserver<MapOuterClass.ReadyResponse> responseObserver) {
        responseObserver.onNext(MapOuterClass.ReadyResponse.newBuilder().setReady(true).build());
        responseObserver.onCompleted();
    }
}
