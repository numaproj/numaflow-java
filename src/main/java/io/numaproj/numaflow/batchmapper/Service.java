package io.numaproj.numaflow.batchmapper;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.batchmap.v1.BatchMapGrpc;
import io.numaproj.numaflow.batchmap.v1.Batchmap;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


@Slf4j
@AllArgsConstructor
class Service extends BatchMapGrpc.BatchMapImplBase {

    // batchMapTaskExecutor is the executor for the batchMap. It is a fixed size thread pool
    // with the number of threads equal to the number of cores on the machine times 2.
    private final ExecutorService batchMapTaskExecutor = Executors
            .newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    // SHUTDOWN_TIME is the time to wait for the sinker to shut down, in seconds.
    // We use 30 seconds as the default value because it provides a balance between giving tasks enough time to complete
    // and not delaying program termination unduly.
    private final long SHUTDOWN_TIME = 30;

    private final BatchMapper batchMapper;

    @Override
    public StreamObserver<Batchmap.BatchMapRequest> batchMapFn(StreamObserver<Batchmap.BatchMapResponse> responseObserver) {

        if (this.batchMapper == null) {
            return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(
                    BatchMapGrpc.getBatchMapFnMethod(),
                    responseObserver);
        }

        DatumIteratorImpl datumStream = new DatumIteratorImpl();

        Future<BatchResponses> result = batchMapTaskExecutor.submit(() -> this.batchMapper.processMessage(
                datumStream));

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
                var status = Status.UNKNOWN.withDescription(throwable.getMessage()).withCause(throwable);
                responseObserver.onError(status.asException());
            }

            @Override
            public void onCompleted() {
                try {
                    // We Fire off the call to the client from here and stream the response back
                    datumStream.writeMessage(HandlerDatum.EOF_DATUM);
                    BatchResponses responses = result.get();
                    log.debug(
                            "Finished the call Result size is :{} and iterator count is :{}",
                            responses.getItems().size(),
                            datumStream.getCount());
                    // Crash if the number of responses from the users don't match the input requests ignoring the EOF message
                    if (responses.getItems().size() != datumStream.getCount() - 1) {
                        throw new RuntimeException("Number of results did not match expected " + (datumStream.getCount()-1) + " but got " + responses.getItems().size());
                    }
                    buildAndStreamResponse(responses, responseObserver);
                } catch (Exception e) {
                    log.error("Error Encountered in batchMap Stream onCompleted", e);
                    onError(e);
                }
            }
        };
    }

    private void buildAndStreamResponse(BatchResponses responses, StreamObserver<Batchmap.BatchMapResponse> responseObserver) {
        responses.getItems().forEach(message -> {
            List<Batchmap.BatchMapResponse.Result> batchMapResponseResult = new ArrayList<>();
            message.getItems().forEach(res -> {
                batchMapResponseResult.add(
                        Batchmap.BatchMapResponse.Result
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
            Batchmap.BatchMapResponse singleRequestResponse = Batchmap.BatchMapResponse
                    .newBuilder()
                    .setId(message.getId())
                    .addAllResults(batchMapResponseResult)
                    .build();
            // Stream the response back to the sender
            responseObserver.onNext(singleRequestResponse);
        });
        responseObserver.onCompleted();
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

    // shuts down the executor service which is used for reduce
    public void shutDown() {
        this.batchMapTaskExecutor.shutdown();
        try {
            if (!batchMapTaskExecutor.awaitTermination(SHUTDOWN_TIME, TimeUnit.SECONDS)) {
                log.error("BatchMap executor did not terminate in the specified time.");
                List<Runnable> droppedTasks = batchMapTaskExecutor.shutdownNow();
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
