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
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
class Service extends SinkGrpc.SinkImplBase {
    // sinkTaskExecutor is the executor for the sinker. It is a fixed size thread pool
    // with the number of threads equal to the number of cores on the machine times 2.
    // We use 2 times the number of cores because the sinker is a CPU intensive task.
    private final ExecutorService sinkTaskExecutor = Executors
            .newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    private final Sinker sinker;
    private final Server server;

    public Service(Sinker sinker, Server server) {
        this.sinker = sinker;
        this.server = server;
    }

    /**
     * Applies a function to each datum element in the stream.
     */
    @Override
    public StreamObserver<SinkOuterClass.SinkRequest> sinkFn(StreamObserver<SinkOuterClass.SinkResponse> responseObserver) {
        return new StreamObserver<>() {
            private boolean startOfStream = true;
            private CompletableFuture<ResponseList> result;
            private DatumIterator datumStream;
            private boolean handshakeDone = false;

            @Override
            public void onNext(SinkOuterClass.SinkRequest request) {
                // make sure the handshake is done before processing the messages
                if (!handshakeDone) {
                    log.info("Handshake hasn't been done yet");
                    if (!request.hasHandshake() || !request.getHandshake().getSot()) {
                        log.error("Handshake request not received, triggering onError on the responseObserver");
                        responseObserver.onError(Status.INVALID_ARGUMENT
                                .withDescription("Handshake request not received")
                                .asException());
                        try {
                            server.stop();
                        } catch (InterruptedException e) {
                            log.error("Error while stopping the server - {}", e.getMessage());
                            throw new RuntimeException(e);
                        }
                        return;
                    }
                    log.info("Handshake request received, sending handshake response");
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
                    // FIXME - null check before new.
                    result = CompletableFuture.supplyAsync(
                            () -> sinker.processMessages(datumStream),
                            sinkTaskExecutor);
                    startOfStream = false;
                }

                try {
                    if (request.hasStatus() && request.getStatus().getEot()) {
                        log.info("End of transmission received, collecting responses and sending back to the client");
                        // End of transmission, write EOF datum to the stream
                        // Wait for the result and send the response back to the client
                        datumStream.write(HandlerDatum.EOF_DATUM);

                        log.info("Collecting responses from the CompletableFuture - result");
                        ResponseList responses = result.join();
                        log.info("Responses collected from the CompletableFuture - result");
                        SinkOuterClass.SinkResponse.Builder responseBuilder = SinkOuterClass.SinkResponse.newBuilder();
                        for (Response response : responses.getResponses()) {
                            responseBuilder.addResults(buildResult(response));
                        }
                        log.info("Sending response back to the client - {}", responseBuilder.build());
                        responseObserver.onNext(responseBuilder.build());

                        // send eot response to indicate end of transmission for the batch
                        SinkOuterClass.SinkResponse eotResponse = SinkOuterClass.SinkResponse
                                .newBuilder()
                                .setStatus(SinkOuterClass.TransmissionStatus
                                        .newBuilder()
                                        .setEot(true)
                                        .build())
                                .build();
                        log.info("Sending EOT response back to the client {}", eotResponse);
                        responseObserver.onNext(eotResponse);
                        log.info("All responses sent back to the client, including the EOT response");

                        // reset the startOfStream flag, since the stream has ended
                        // so that the next request will be treated as the start of the stream
                        startOfStream = true;
                        log.info("startOfStream flag reset to true");
                    } else {
                        datumStream.write(constructHandlerDatum(request));
                    }
                } catch (CompletionException ce) {
                    Throwable cause = ce.getCause(); // Get the original RuntimeException, if any.
                    log.error("Error occurred while processing messages: {}", cause.getMessage());
                    responseObserver.onError(cause); // Pass the error back to the client.

                    // Initiate server shutdown.
                    try {
                        server.stop();
                    } catch (InterruptedException ex) {
                        log.error("Error while stopping the server: {}", ex.getMessage());
                    }
                } catch (Exception e) {
                    log.error("Encountered error in sink onNext - {}", e.getMessage());
                    responseObserver.onError(e);
                    // Initiate server shutdown.
                    try {
                        server.stop();
                    } catch (InterruptedException ex) {
                        log.error("Error while stopping the server - {}", ex.getMessage());
                        throw new RuntimeException(ex);
                    }
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("Encountered error in sinkFn - {}", throwable.getMessage());
                responseObserver.onError(throwable);
                try {
                    server.stop();
                } catch (InterruptedException e) {
                    log.info("Error while stopping the server - {}", e.getMessage());
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    private SinkOuterClass.SinkResponse.Result buildResult(Response response) {
        SinkOuterClass.Status status = response.getFallback() ? SinkOuterClass.Status.FALLBACK :
                response.getSuccess() ? SinkOuterClass.Status.SUCCESS : SinkOuterClass.Status.FAILURE;
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
                d.getRequest().getHeadersMap()
        );
    }

    // shuts down the executor service. it's a blocking call until all the tasks are shut down .
    public void shutDown() {
        log.info("Shutting down sink executors.");
        // TODO - do we need call this one if we call awaitTermination below?
        this.sinkTaskExecutor.shutdown();
        try {
            // SHUTDOWN_TIME is the time to wait for the sinker to shut down, in seconds.
            // We use 30 seconds as the default value because it provides a balance between giving tasks enough time to complete
            // and not delaying program termination unduly.
            long SHUTDOWN_TIME = 30;

            if (!sinkTaskExecutor.awaitTermination(SHUTDOWN_TIME, TimeUnit.SECONDS)) {
                log.error("Sink executor did not terminate in the specified time.");
                List<Runnable> droppedTasks = sinkTaskExecutor.shutdownNow();
                log.error(
                        "Sink executor was abruptly shut down. {} tasks will not be executed.",
                        droppedTasks.size());
            }

            while(!sinkTaskExecutor.awaitTermination(SHUTDOWN_TIME, TimeUnit.SECONDS)) {
                log.error("Sink executor did not terminate in the specified time. Keep retrying.");
            }
            log.info("Sink executor was terminated.");
        } catch (InterruptedException e) {
            // this one clears the interrupted status - I don't think it's not good.
            if (Thread.interrupted()) {
                System.err.println("Thread was interrupted when trying to stop the sink service task executor.\n"
                        + "Thread interrupted status cleared");
            }
            System.err.println("Sink service printing stack trace for the exception to stderr");
            e.printStackTrace(System.err);
        }
    }
}
