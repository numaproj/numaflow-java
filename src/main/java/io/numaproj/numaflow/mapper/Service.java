package io.numaproj.numaflow.mapper;

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

@Slf4j
@AllArgsConstructor
class Service extends MapGrpc.MapImplBase {

    public static final ActorSystem mapperActorSystem = ActorSystem.create("mapper");

    private final Mapper mapper;
    private final CompletableFuture<Void> shutdownSignal;

    @Override
    public StreamObserver<MapOuterClass.MapRequest> mapFn(
            final StreamObserver<MapOuterClass.MapResponse> responseObserver) {

        if (this.mapper == null) {
            return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(
                    getMapFnMethod(),
                    responseObserver);
        }

        // create a MapSupervisorActor that processes the map requests.
        ActorRef mapSupervisorActor = mapperActorSystem
                .actorOf(MapSupervisorActor.props(mapper, responseObserver, shutdownSignal));

        return new StreamObserver<>() {
            private boolean handshakeDone = false;

            @Override
            public void onNext(MapOuterClass.MapRequest request) {
                // make sure the handshake is done before processing the messages
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
                    return;
                }
                // send the message to the MapSupervisorActor.
                mapSupervisorActor.tell(request, ActorRef.noSender());
            }

            @Override
            public void onError(Throwable throwable) {
                mapSupervisorActor.tell(new Exception(throwable), ActorRef.noSender());
            }

            @Override
            public void onCompleted() {
                // indicate the end of input to the MapSupervisorActor.
                mapSupervisorActor.tell(Constants.EOF, ActorRef.noSender());
            }
        };
    }

    /**
     * IsReady is the heartbeat endpoint for gRPC.
     */
    @Override
    public void isReady(
            Empty request,
            StreamObserver<MapOuterClass.ReadyResponse> responseObserver) {
        responseObserver.onNext(MapOuterClass.ReadyResponse.newBuilder().setReady(true).build());
        responseObserver.onCompleted();
    }
}
