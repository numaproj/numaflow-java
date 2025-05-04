package io.numaproj.numaflow.sinker;

import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.google.rpc.Code;
import com.google.rpc.DebugInfo;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.shared.ExceptionUtils;
import io.numaproj.numaflow.sink.v1.SinkGrpc;
import io.numaproj.numaflow.sink.v1.SinkOuterClass;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
class Service extends SinkGrpc.SinkImplBase {
    // sinkTaskExecutor is the executor for the sinker. It is a fixed size thread
    // pool
    // with the number of threads equal to the number of cores on the machine times
    // 2.
    // We use 2 times the number of cores because the sinker is a CPU intensive
    // task.
    private final ExecutorService sinkTaskExecutor = Executors
            .newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    private final Sinker sinker;
    private final CompletableFuture<Void> shutdownSignal;

    /**
     * Applies a function to each datum element in the stream.
     */
    @Override
    public StreamObserver<SinkOuterClass.SinkRequest> sinkFn(
            StreamObserver<SinkOuterClass.SinkResponse> responseObserver) {
        return new StreamObserver<>() {
            private boolean startOfStream = true;
            private CompletableFuture<ResponseList> result;
            private DatumIteratorImpl datumStream;
            private boolean handshakeDone = false;

            @Override
            public void onNext(SinkOuterClass.SinkRequest request) {
                // make sure the handshake is done before processing the messages
                if (!handshakeDone) {
                    if (!request.hasHandshake() || !request.getHandshake().getSot()) {
                        responseObserver.onError(Status.INVALID_ARGUMENT
                                .withDescription("Handshake request not received")
                                .asException());
                        return;
                    }
                    responseObserver.onNext(SinkOuterClass.SinkResponse.newBuilder()
                            .setHandshake(request.getHandshake())
                            .build());
                    handshakeDone = true;
                    return;
                }

                // Create a DatumIterator to write the messages to the sinker
                // and start the sinker if it is the start of the stream
                if (startOfStream) {
                    datumStream = new DatumIteratorImpl();
                    result = CompletableFuture.supplyAsync(
                            () -> sinker.processMessages(datumStream),
                            sinkTaskExecutor);
                    startOfStream = false;
                }

                try {
                    if (request.hasStatus() && request.getStatus().getEot()) {
                        // End of transmission, write EOF datum to the stream
                        // Wait for the result and send the response back to the client
                        datumStream.writeMessage(HandlerDatum.EOF_DATUM);
                        ResponseList responses = result.join();

                        if (responses != null) {
                            SinkOuterClass.SinkResponse.Builder responseBuilder = SinkOuterClass.SinkResponse.newBuilder();
                            for (Response response : responses.getResponses()) {
                                responseBuilder.addResults(buildResult(response));
                            }
                            responseObserver.onNext(responseBuilder.build());
                        }

                        // send eot response to indicate end of transmission for the batch
                        SinkOuterClass.SinkResponse eotResponse = SinkOuterClass.SinkResponse
                                .newBuilder()
                                .setStatus(SinkOuterClass.TransmissionStatus
                                        .newBuilder()
                                        .setEot(true)
                                        .build())
                                .build();
                        responseObserver.onNext(eotResponse);

                        // reset the startOfStream flag, since the stream has ended
                        // so that the next request will be treated as the start of the stream
                        startOfStream = true;
                    } else {
                        datumStream.writeMessage(constructHandlerDatum(request));
                    }
                } catch (Exception e) {
                    log.error("Encountered error in sinkFn onNext", e);
                    // Build gRPC Status
                    com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                            .setCode(Code.INTERNAL.getNumber())
                            .setMessage(ExceptionUtils.getExceptionErrorString() + ": "
                                    + (e.getMessage() != null ? e.getMessage() : ""))
                            .addDetails(Any.pack(DebugInfo.newBuilder()
                                    .setDetail(ExceptionUtils.getStackTrace(e))
                                    .build()))
                            .build();
                    responseObserver.onError(StatusProto.toStatusRuntimeException(status));
                    shutdownSignal.completeExceptionally(e);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("Encountered error in sinkFn", throwable);
                shutdownSignal.completeExceptionally(throwable);
                responseObserver.onError(Status.INTERNAL
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

    private SinkOuterClass.SinkResponse.Result buildResult(Response response) {
        SinkOuterClass.Status status = response.getFallback() ? SinkOuterClass.Status.FALLBACK
                : response.getSuccess() ? SinkOuterClass.Status.SUCCESS : SinkOuterClass.Status.FAILURE;
        return SinkOuterClass.SinkResponse.Result.newBuilder()
                .setId(response.getId() == null ? "" : response.getId())
                .setErrMsg(response.getErr() == null ? "" : response.getErr())
                .setStatus(status)
                .build();
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
                d.getRequest().getKeysList().toArray(new String[0]),
                d.getRequest().getValue().toByteArray(),
                Instant.ofEpochSecond(
                        d.getRequest().getWatermark().getSeconds(),
                        d.getRequest().getWatermark().getNanos()),
                Instant.ofEpochSecond(
                        d.getRequest().getEventTime().getSeconds(),
                        d.getRequest().getEventTime().getNanos()),
                d.getRequest().getId(),
                d.getRequest().getHeadersMap());
    }

    // shuts down the executor service
    public void shutDown() {
        this.sinkTaskExecutor.shutdown();
        try {
            // SHUTDOWN_TIME is the time to wait for the sinker to shut down, in seconds.
            // We use 30 seconds as the default value because it provides a balance between
            // giving tasks enough time to complete
            // and not delaying program termination unduly.
            long SHUTDOWN_TIME = 30;

            if (!sinkTaskExecutor.awaitTermination(SHUTDOWN_TIME, TimeUnit.SECONDS)) {
                log.error("Sink executor did not terminate in the specified time.");
                List<Runnable> droppedTasks = sinkTaskExecutor.shutdownNow();
                log.error(
                        "Sink executor was abruptly shut down. {} tasks will not be executed.",
                        droppedTasks.size());
            } else {
                log.info("Sink executor was terminated.");
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
            e.printStackTrace();
        }
    }
}
