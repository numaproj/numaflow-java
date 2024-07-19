package io.numaproj.numaflow.batchmapper;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AllDeadLetters;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.batchmap.v1.BatchMapGrpc;
import io.numaproj.numaflow.batchmap.v1.Batchmap;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;


@Slf4j
@AllArgsConstructor
class Service extends BatchMapGrpc.BatchMapImplBase {

    private final BatchMapper batchMapper;
    public static final ActorSystem batchMapActorSystem = ActorSystem.create("batchmap");


    static void handleFailure(
            CompletableFuture<Void> failedFuture,
            StreamObserver<Batchmap.BatchMapResponse> responseStreamObserver) {
        new Thread(() -> {
            try {
                failedFuture.get();
            } catch (Exception e) {
                e.printStackTrace();
                var status = Status.UNKNOWN.withDescription(e.getMessage()).withCause(e);
                responseStreamObserver.onError(status.asException());
            }
        }).start();

    }

    @Override
    public StreamObserver<Batchmap.BatchMapRequest> batchMapFn(StreamObserver<Batchmap.BatchMapResponse> responseObserver) {

        if (this.batchMapper == null) {
            return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(
                    BatchMapGrpc.getBatchMapFnMethod(),
                    responseObserver);
        }

        DatumIteratorImpl datumStream = new DatumIteratorImpl();

        CompletableFuture<Void> failedFuture = new CompletableFuture<>();

        // create a shutdown actor that listens to exceptions.
        ActorRef shutdownActorRef = batchMapActorSystem.actorOf(BatchMapShutdownActor.props(
                failedFuture));

        // subscribe for dead letters
        batchMapActorSystem.getEventStream().subscribe(shutdownActorRef, AllDeadLetters.class);

        handleFailure(failedFuture, responseObserver);

        ActorRef batchMapActor = batchMapActorSystem
                .actorOf(BatchMapActor.props(
                        batchMapper,
                        shutdownActorRef,
                        responseObserver
                ));

        return new StreamObserver<Batchmap.BatchMapRequest>() {
            @Override
            public void onNext(Batchmap.BatchMapRequest mapRequest) {
                try {
                    datumStream.writeMessage(constructHandlerDatum(mapRequest));
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    onError(e);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                // We close the stream and let the sender retry the messages
                log.error("Error Encountered in batchMap Stream", throwable);
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                try {
                    // We Fire off the call to the client from here and stream the response back
                    datumStream.writeMessage(HandlerDatum.EOF_DATUM);
                    batchMapActor.tell(datumStream, ActorRef.noSender());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    onError(e);
                }
            }
        };
    }


    @Override
    public void isReady(
            Empty request,
            StreamObserver<Batchmap.ReadyResponse> responseObserver) {
        responseObserver.onNext(Batchmap.ReadyResponse.newBuilder().setReady(true).build());
        responseObserver.onCompleted();
    }

    private HandlerDatum constructHandlerDatum(Batchmap.BatchMapRequest d) {
        return new HandlerDatum(
                d.getKeysList().toArray(new String[0]),
                d.getValue().toByteArray(),
                Instant.ofEpochSecond(
                        d.getWatermark().getSeconds(),
                        d.getWatermark().getNanos()),
                Instant.ofEpochSecond(
                        d.getEventTime().getSeconds(),
                        d.getEventTime().getNanos()),
                d.getId(),
                d.getHeadersMap()
        );
    }

}
