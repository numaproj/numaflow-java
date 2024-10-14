package io.numaproj.numaflow.batchmapper;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.map.v1.MapGrpc;
import io.numaproj.numaflow.map.v1.MapOuterClass;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@AllArgsConstructor
class Service extends MapGrpc.MapImplBase {

    // Executor service for the batch mapper. It is a fixed size thread pool
    // with the number of threads equal to the number of cores on the machine times 2.
    // We use 2 times the number of cores because the batch mapper is a CPU intensive task.
    private final ExecutorService mapTaskExecutor = Executors
            .newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    // Time to wait for the batch mapper to shut down, in seconds.
    // We use 30 seconds as the default value because it provides a balance between giving tasks enough time to complete
    // and not delaying program termination unduly.
    private final long SHUTDOWN_TIME = 30;

    // BatchMapper instance to process the messages
    private final BatchMapper batchMapper;

    // Applies a map function to each datum element in the stream.
    @Override
    public StreamObserver<MapOuterClass.MapRequest> mapFn(StreamObserver<MapOuterClass.MapResponse> responseObserver) {

        // If the batchMapper is null, return an unimplemented call
        if (this.batchMapper == null) {
            return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(
                    MapGrpc.getMapFnMethod(),
                    responseObserver);
        }

        // Return a new StreamObserver to handle the incoming requests
        return new StreamObserver<>() {
            private boolean startOfStream = true;
            private boolean handshakeDone = false;
            private DatumIteratorImpl datumStream;
            private CompletableFuture<BatchResponses> result;

            // Called for each incoming request
            @Override
            public void onNext(MapOuterClass.MapRequest mapRequest) {
                try {
                    // Make sure the handshake is done before processing the messages
                    if (!handshakeDone) {
                        if (!mapRequest.hasHandshake() || !mapRequest.getHandshake().getSot()) {
                            responseObserver.onError(Status.INVALID_ARGUMENT
                                    .withDescription("Handshake request not received")
                                    .asException());
                            return;
                        }
                        responseObserver.onNext(MapOuterClass.MapResponse.newBuilder()
                                .setHandshake(mapRequest.getHandshake())
                                .build());
                        handshakeDone = true;
                        return;
                    }

                    // Create a DatumIterator to write the messages to the batch mapper
                    // and start the batch mapper if it is the start of the stream
                    if (startOfStream) {
                        datumStream = new DatumIteratorImpl();
                        result = CompletableFuture.supplyAsync(
                                () -> batchMapper.processMessage(datumStream),
                                mapTaskExecutor);
                        startOfStream = false;
                    }

                    // If end of transmission, write EOF datum to the stream
                    // Wait for the result and send the response back to the client
                    if (mapRequest.hasStatus() && mapRequest.getStatus().getEot()) {
                        datumStream.writeMessage(HandlerDatum.EOF_DATUM);
                        BatchResponses responses = result.join();
                        buildAndStreamResponse(responses, responseObserver);
                        startOfStream = true;
                    } else {
                        datumStream.writeMessage(constructHandlerDatum(mapRequest));
                    }
                } catch (Exception e) {
                    log.error("Encountered an error in batch map", e);
                    responseObserver.onError(Status.UNKNOWN
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asException());
                }
            }

            // Called when an error occurs
            @Override
            public void onError(Throwable throwable) {
                log.error("Error Encountered in batchMap Stream", throwable);
                var status = Status.UNKNOWN
                        .withDescription(throwable.getMessage())
                        .withCause(throwable);
                responseObserver.onError(status.asException());
            }

            // Called when the client has finished sending requests
            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    // Build and stream the response back to the client
    private void buildAndStreamResponse(
            BatchResponses responses,
            StreamObserver<MapOuterClass.MapResponse> responseObserver) {
        responses.getItems().forEach(message -> {
            List<MapOuterClass.MapResponse.Result> mapResponseResult = new ArrayList<>();
            message.getItems().forEach(res -> {
                mapResponseResult.add(
                        MapOuterClass.MapResponse.Result
                                .newBuilder()
                                .setValue(res.getValue()
                                        == null ? ByteString.EMPTY : ByteString.copyFrom(
                                        res.getValue()))
                                .addAllKeys(res.getKeys()
                                        == null ? new ArrayList<>() : List.of(res.getKeys()))
                                .addAllTags(res.getTags()
                                        == null ? new ArrayList<>() : List.of(res.getTags()))
                                .build()
                );
            });
            MapOuterClass.MapResponse singleRequestResponse = MapOuterClass.MapResponse
                    .newBuilder()
                    .setId(message.getId())
                    .addAllResults(mapResponseResult)
                    .build();
            responseObserver.onNext(singleRequestResponse);
        });
        // Send an EOT message to indicate the end of the transmission for the batch.
        MapOuterClass.MapResponse eotResponse = MapOuterClass.MapResponse
                .newBuilder()
                .setStatus(MapOuterClass.TransmissionStatus.newBuilder().setEot(true).build())
                .build();
        responseObserver.onNext(eotResponse);
        responseObserver.onCompleted();
    }

    // IsReady is the heartbeat endpoint for gRPC.
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
                d.getRequest().getKeysList().toArray(new String[0]),
                d.getRequest().getValue().toByteArray(),
                Instant.ofEpochSecond(
                        d.getRequest().getWatermark().getSeconds(),
                        d.getRequest().getWatermark().getNanos()),
                Instant.ofEpochSecond(
                        d.getRequest().getEventTime().getSeconds(),
                        d.getRequest().getEventTime().getNanos()),
                d.getId(),
                d.getRequest().getHeadersMap()
        );
    }

    // Shuts down the executor service which is used for batch map
    public void shutDown() {
        this.mapTaskExecutor.shutdown();
        try {
            if (!mapTaskExecutor.awaitTermination(SHUTDOWN_TIME, TimeUnit.SECONDS)) {
                log.error("BatchMap executor did not terminate in the specified time.");
                List<Runnable> droppedTasks = mapTaskExecutor.shutdownNow();
                log.error("BatchMap executor was abruptly shut down. " + droppedTasks.size()
                        + " tasks will not be executed.");
            } else {
                log.info("BatchMap executor was terminated.");
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
            e.printStackTrace();
        }
    }
}
