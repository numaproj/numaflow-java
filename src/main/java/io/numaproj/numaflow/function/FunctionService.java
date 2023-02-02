package io.numaproj.numaflow.function;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.function.map.MapHandler;
import io.numaproj.numaflow.function.metadata.IntervalWindow;
import io.numaproj.numaflow.function.metadata.IntervalWindowImpl;
import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.metadata.MetadataImpl;
import io.numaproj.numaflow.function.reduce.ReduceDatumStream;
import io.numaproj.numaflow.function.reduce.ReduceDatumStreamImpl;
import io.numaproj.numaflow.function.reduce.ReduceHandler;
import io.numaproj.numaflow.function.v1.Udfunction;
import io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc.getMapFnMethod;
import static io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc.getReduceFnMethod;

class FunctionService extends UserDefinedFunctionGrpc.UserDefinedFunctionImplBase {
    private static final Logger logger = Logger.getLogger(FunctionService.class.getName());
    // it will never be smaller than one
    private final ExecutorService reduceTaskExecutor = Executors
            .newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    private final long SHUTDOWN_TIME = 30;

    private MapHandler mapHandler;
    private ReduceHandler reduceHandler;
    private StreamObserver<Udfunction.Datum> streamObserver;

    public FunctionService() {
    }

    public void setMapHandler(MapHandler mapHandler) {
        this.mapHandler = mapHandler;
    }

    public void setReduceHandler(ReduceHandler reduceHandler) {
        this.reduceHandler = reduceHandler;
    }

    /**
     * Applies a function to each datum element.
     */
    @Override
    public void mapFn(
            Udfunction.Datum request,
            StreamObserver<Udfunction.DatumList> responseObserver) {
        if (this.mapHandler==null) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(
                    getMapFnMethod(),
                    responseObserver);
            return;
        }

        // get key from gPRC metadata
        String key = Function.DATUM_CONTEXT_KEY.get();

        // get Datum from request
        Udfunction.Datum handlerDatum = Udfunction.Datum.newBuilder()
                .setValue(request.getValue())
                .setEventTime(request.getEventTime())
                .setWatermark(request.getWatermark())
                .build();

        // process Datum
        Message[] messages = mapHandler.HandleDo(key, handlerDatum);

        // set response
        responseObserver.onNext(buildDatumList(messages));
        responseObserver.onCompleted();
    }

    /**
     * Streams input data to reduceFn and returns the result.
     */
    @Override
    public StreamObserver<Udfunction.Datum> reduceFn(StreamObserver<Udfunction.DatumList> responseObserver) {
        if (this.reduceHandler==null) {
            return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(
                    getReduceFnMethod(),
                    responseObserver);
        }

        ReduceDatumStreamImpl reduceDatumStreamImpl = new ReduceDatumStreamImpl();

        // get key from gPRC metadata
        String key = Function.DATUM_CONTEXT_KEY.get();

        // get window start and end time from gPRC metadata
        String winSt = Function.WINDOW_START_TIME.get();
        String winEt = Function.WINDOW_END_TIME.get();

        // convert the start and end time to Instant
        Instant startTime = Instant.ofEpochMilli(Long.parseLong(winSt));
        Instant endTime = Instant.ofEpochMilli(Long.parseLong(winEt));

        // create metadata
        IntervalWindow iw = new IntervalWindowImpl(startTime, endTime);
        Metadata md = new MetadataImpl(iw);

        Future<Message[]> result = reduceTaskExecutor.submit(() -> reduceHandler.HandleDo(
                key,
                reduceDatumStreamImpl,
                md));

        return new StreamObserver<Udfunction.Datum>() {
            @Override
            public void onNext(Udfunction.Datum datum) {
                // get Datum from request
                Udfunction.Datum handlerDatum = Udfunction.Datum.newBuilder()
                        .setValue(datum.getValue())
                        .setEventTime(datum.getEventTime())
                        .setWatermark(datum.getWatermark())
                        .build();

                try {
                    reduceDatumStreamImpl.WriteMessage(handlerDatum);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    onError(e);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.log(Level.WARNING, "Encountered error in reduceFn", throwable);
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                Udfunction.DatumList response = Udfunction.DatumList
                        .newBuilder()
                        .getDefaultInstanceForType();
                try {
                    reduceDatumStreamImpl.WriteMessage(ReduceDatumStream.EOF);
                    // wait until the reduce handler returns, result.get() is a blocking call
                    Message[] resultMessages = result.get();
                    // construct datumList from resultMessages
                    response = buildDatumList(resultMessages);

                } catch (InterruptedException | ExecutionException e) {
                    Thread.interrupted();
                    onError(e);
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
    public void isReady(Empty request, StreamObserver<Udfunction.ReadyResponse> responseObserver) {
        responseObserver.onNext(Udfunction.ReadyResponse.newBuilder().setReady(true).build());
        responseObserver.onCompleted();
    }

    /**
     * shuts down the executor service which is used for reduce.
     */
    public void shutDown() {
        this.reduceTaskExecutor.shutdown();
        try {
            if (!reduceTaskExecutor.awaitTermination(SHUTDOWN_TIME, TimeUnit.SECONDS)) {
                System.err.println("Reduce executor did not terminate in the specified time.");
                List<Runnable> droppedTasks = reduceTaskExecutor.shutdownNow();
                System.err.println("Reduce executor was abruptly shut down. " + droppedTasks.size()
                        + " tasks will not be executed.");
            } else {
                System.err.println("Reduce executor was terminated.");
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
            e.printStackTrace();
        }
    }

    public Udfunction.DatumList buildDatumList(Message[] messages) {
        Udfunction.DatumList.Builder datumListBuilder = Udfunction.DatumList.newBuilder();
        Arrays.stream(messages).forEach(message -> {
            datumListBuilder.addElements(Udfunction.Datum.newBuilder()
                    .setKey(message.getKey())
                    .setValue(ByteString.copyFrom(message.getValue()))
                    .build());
        });
        return datumListBuilder.build();
    }
}
