package io.numaproj.numaflow.sinker;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sink.v1.SinkGrpc;
import io.numaproj.numaflow.sink.v1.SinkOuterClass;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.numaproj.numaflow.sink.v1.SinkGrpc.getSinkFnMethod;

@Slf4j
class Service extends SinkGrpc.SinkImplBase {
    // sinkTaskExecutor is the executor for the sinker. It is a fixed size thread pool
    // with the number of threads equal to the number of cores on the machine times 2.
    // We use 2 times the number of cores because the sinker is a CPU intensive task.
    private final ExecutorService sinkTaskExecutor = Executors
            .newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    // SHUTDOWN_TIME is the time to wait for the sinker to shut down, in seconds.
    // We use 30 seconds as the default value because it provides a balance between giving tasks enough time to complete
    // and not delaying program termination unduly.
    private final long SHUTDOWN_TIME = 30;


    private final Sinker sinker;
    private final CompletableFuture<Void> shutdownSignal;

    public Service(Sinker sinker, CompletableFuture<Void> shutdownSignal) {
        this.sinker = sinker;
        this.shutdownSignal = shutdownSignal;
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

        DatumIteratorImpl datumStream = new DatumIteratorImpl();

        Future<ResponseList> result = sinkTaskExecutor.submit(() -> this.sinker.processMessages(
                datumStream));

        return new StreamObserver<>() {
            @Override
            public void onNext(SinkOuterClass.SinkRequest d) {
                try {
                    datumStream.writeMessage(constructHandlerDatum(d));
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    onError(e);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("Encountered error in sinkFn - {}", throwable.getMessage());
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                SinkOuterClass.SinkResponse response = SinkOuterClass.SinkResponse
                        .newBuilder()
                        .build();
                try {
                    datumStream.writeMessage(HandlerDatum.EOF_DATUM);
                    // wait until the sink handler returns, result.get() is a blocking call
                    ResponseList responses = result.get();
                    // construct responseList from responses
                    response = buildResponseList(responses);

                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    shutdownSignal.completeExceptionally(e);
                    responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
                }
                responseObserver.onNext(response);
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
            StreamObserver<SinkOuterClass.ReadyResponse> responseObserver) {
        responseObserver.onNext(SinkOuterClass.ReadyResponse.newBuilder().setReady(true).build());
        responseObserver.onCompleted();
    }

    private HandlerDatum constructHandlerDatum(SinkOuterClass.SinkRequest d) {
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

    public SinkOuterClass.SinkResponse buildResponseList(ResponseList responses) {
        var responseBuilder = SinkOuterClass.SinkResponse.newBuilder();
        responses.getResponses().forEach(response -> {
            SinkOuterClass.Status status = response.getFallback() ? SinkOuterClass.Status.FALLBACK :
                    response.getSuccess() ? SinkOuterClass.Status.SUCCESS : SinkOuterClass.Status.FAILURE;
            responseBuilder.addResults(SinkOuterClass.SinkResponse.Result.newBuilder()
                    .setId(response.getId() == null ? "" : response.getId())
                    .setErrMsg(response.getErr() == null ? "" : response.getErr())
                    .setStatus(status)
                    .build());
        });
        return responseBuilder.build();
    }

    // shuts down the executor service which is used for reduce
    public void shutDown() {
        this.sinkTaskExecutor.shutdown();
        try {
            if (!sinkTaskExecutor.awaitTermination(SHUTDOWN_TIME, TimeUnit.SECONDS)) {
                log.error("Sink executor did not terminate in the specified time.");
                List<Runnable> droppedTasks = sinkTaskExecutor.shutdownNow();
                log.error("Sink executor was abruptly shut down. " + droppedTasks.size()
                        + " tasks will not be executed.");
            } else {
                log.info("Sink executor was terminated.");
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
            e.printStackTrace();
        }
    }
}
