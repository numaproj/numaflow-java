package io.numaproj.numaflow.sink;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sink.v1.Udsink;
import io.numaproj.numaflow.sink.v1.UserDefinedSinkGrpc;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc.getMapFnMethod;

@Slf4j
class SinkService extends UserDefinedSinkGrpc.UserDefinedSinkImplBase {
    // it will never be smaller than one
    private final ExecutorService sinkTaskExecutor = Executors
            .newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    private final long SHUTDOWN_TIME = 30;
    private SinkHandler sinkHandler;

    public SinkService() {
    }

    public void setSinkHandler(SinkHandler sinkHandler) {
        this.sinkHandler = sinkHandler;
    }

    /**
     * Applies a function to each datum element in the stream.
     */
    @Override
    public StreamObserver<Udsink.DatumRequest> sinkFn(StreamObserver<Udsink.ResponseList> responseObserver) {
        if (this.sinkHandler == null) {
            return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(
                    getMapFnMethod(),
                    responseObserver);
        }
        SinkDatumStreamImpl sinkDatumStream = new SinkDatumStreamImpl();

        Future<ResponseList> result = sinkTaskExecutor.submit(() -> sinkHandler.processMessage(
                sinkDatumStream));

        return new StreamObserver<Udsink.DatumRequest>() {
            @Override
            public void onNext(Udsink.DatumRequest d) {
                // get Datum from request
                HandlerDatum handlerDatum = new HandlerDatum(
                        d.getKeysList().toArray(new String[0]),
                        d.getValue().toByteArray(),
                        Instant.ofEpochSecond(
                                d.getWatermark().getWatermark().getSeconds(),
                                d.getWatermark().getWatermark().getNanos()),
                        Instant.ofEpochSecond(
                                d.getEventTime().getEventTime().getSeconds(),
                                d.getEventTime().getEventTime().getNanos()),
                        d.getId(),
                        false);

                try {
                    sinkDatumStream.WriteMessage(handlerDatum);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    onError(e);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.warn("Encountered error in sinkFn - {}", throwable.getMessage());
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                Udsink.ResponseList response = Udsink.ResponseList
                        .newBuilder()
                        .getDefaultInstanceForType();
                try {
                    sinkDatumStream.WriteMessage(SinkDatumStream.EOF);
                    // wait until the sink handler returns, result.get() is a blocking call
                    ResponseList responses = result.get();
                    // construct responseList from responses
                    response = buildResponseList(responses);

                } catch (InterruptedException | ExecutionException e) {
                    onError(e);
                }
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };
    }

    // shuts down the executor service which is used for reduce
    public void shutDown() {
        this.sinkTaskExecutor.shutdown();
        try {
            if (!sinkTaskExecutor.awaitTermination(SHUTDOWN_TIME, TimeUnit.SECONDS)) {
                System.err.println("Sink executor did not terminate in the specified time.");
                List<Runnable> droppedTasks = sinkTaskExecutor.shutdownNow();
                System.err.println("Sink executor was abruptly shut down. " + droppedTasks.size()
                        + " tasks will not be executed.");
            } else {
                System.err.println("Sink executor was terminated.");
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
            e.printStackTrace();
        }
    }

    /**
     * IsReady is the heartbeat endpoint for gRPC.
     */
    @Override
    public void isReady(Empty request, StreamObserver<Udsink.ReadyResponse> responseObserver) {
        responseObserver.onNext(Udsink.ReadyResponse.newBuilder().setReady(true).build());
        responseObserver.onCompleted();
    }

    public Udsink.ResponseList buildResponseList(ResponseList responses) {
        var responseListBuilder = Udsink.ResponseList.newBuilder();
        responses.getResponses().forEach(response -> {
            responseListBuilder.addResponses(Udsink.Response.newBuilder()
                    .setId(response.getId())
                    .setSuccess(response.getSuccess())
                    .setErrMsg(response.getErr())
                    .build());
        });
        return responseListBuilder.build();
    }
}
