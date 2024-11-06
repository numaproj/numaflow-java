package io.numaproj.numaflow.mapstreamer;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.map.v1.MapGrpc;
import io.numaproj.numaflow.map.v1.MapOuterClass;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static io.numaproj.numaflow.map.v1.MapGrpc.getMapFnMethod;

@Slf4j
@AllArgsConstructor
class Service extends MapGrpc.MapImplBase {

    private final MapStreamer mapStreamer;
    private final CompletableFuture<Void> shutdownSignal;

    @Override
    public StreamObserver<MapOuterClass.MapRequest> mapFn(StreamObserver<MapOuterClass.MapResponse> responseObserver) {

        if (this.mapStreamer == null) {
            return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(
                    getMapFnMethod(),
                    responseObserver);
        }

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

                try {
                    // process the message
                    mapStreamer.processMessage(
                            request
                                    .getRequest()
                                    .getKeysList()
                                    .toArray(new String[0]),
                            constructHandlerDatum(request),
                            new OutputObserverImpl(responseObserver));
                } catch (Exception e) {
                    log.error("Encountered error in mapFn onNext - {}", e.getMessage());
                    shutdownSignal.completeExceptionally(e);
                    responseObserver.onError(Status.INTERNAL
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asException());
                    return;
                }

                // Send an EOT message to indicate the end of the transmission for the batch.
                MapOuterClass.MapResponse eotResponse = MapOuterClass.MapResponse
                        .newBuilder()
                        .setStatus(MapOuterClass.TransmissionStatus
                                .newBuilder()
                                .setEot(true)
                                .build()).build();
                responseObserver.onNext(eotResponse);
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("Encountered error in mapStream Stream - {}", throwable.getMessage());
                shutdownSignal.completeExceptionally(throwable);
                responseObserver.onError(Status.UNKNOWN
                        .withDescription(throwable.getMessage())
                        .withCause(throwable)
                        .asException());
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
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

    // Construct a HandlerDatum from a MapRequest
    private HandlerDatum constructHandlerDatum(MapOuterClass.MapRequest d) {
        return new HandlerDatum(
                d.getRequest().getValue().toByteArray(),
                Instant.ofEpochSecond(
                        d.getRequest().getWatermark().getSeconds(),
                        d.getRequest().getWatermark().getNanos()),
                Instant.ofEpochSecond(
                        d.getRequest().getEventTime().getSeconds(),
                        d.getRequest().getEventTime().getNanos()),
                d.getRequest().getHeadersMap()
        );
    }
}
