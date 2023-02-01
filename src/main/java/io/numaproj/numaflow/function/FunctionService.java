package io.numaproj.numaflow.function;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import akka.dispatch.OnComplete;
import akka.pattern.Patterns;
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
import io.numaproj.numaflow.function.reduce.SimpleActor;
import io.numaproj.numaflow.function.v1.Udfunction;
import io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc;
import scala.concurrent.Future;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc.getMapFnMethod;
import static io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc.getReduceFnMethod;

class FunctionService extends UserDefinedFunctionGrpc.UserDefinedFunctionImplBase {

    private static final Logger logger = Logger.getLogger(FunctionService.class.getName());
    private static final ActorSystem actorSystem = ActorSystem.create("test-system");


    private MapHandler mapHandler;
    private ReduceHandler reduceHandler;

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
        if (this.mapHandler == null) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(
                    getMapFnMethod(),
                    responseObserver);
            return;
        }

        // get key from gPRC metadata
        String key = Function.DATUM_CONTEXT_KEY.get();

        // get Datum from request
        HandlerDatum handlerDatum = new HandlerDatum(
                request.getValue().toByteArray(),
                Instant.ofEpochSecond(
                        request.getWatermark().getWatermark().getSeconds(),
                        request.getWatermark().getWatermark().getNanos()),
                Instant.ofEpochSecond(
                        request.getEventTime().getEventTime().getSeconds(),
                        request.getEventTime().getEventTime().getNanos()));

        // process Datum
        Message[] messages = mapHandler.HandleDo(key, handlerDatum);

        // set response
        responseObserver.onNext(buildDatumListResponse(messages));
        responseObserver.onCompleted();
    }

    /**
     * Streams input data to reduceFn and returns the result.
     */
    @Override
    public StreamObserver<Udfunction.Datum> reduceFn(StreamObserver<Udfunction.DatumList> responseObserver) {
        ConcurrentHashMap<String, ReduceDatumStreamImpl> streamMap = new ConcurrentHashMap();
        List<ActorRef> actorList = new ArrayList<>();
        if (this.reduceHandler == null) {
            return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(
                    getReduceFnMethod(),
                    responseObserver);
        }

        // get window start and end time from gPRC metadata
        String winSt = Function.WINDOW_START_TIME.get();
        String winEt = Function.WINDOW_END_TIME.get();

        // convert the start and end time to Instant
        Instant startTime = Instant.ofEpochMilli(Long.parseLong(winSt));
        Instant endTime = Instant.ofEpochMilli(Long.parseLong(winEt));

        // create metadata
        IntervalWindow iw = new IntervalWindowImpl(startTime, endTime);
        Metadata md = new MetadataImpl(iw);

        List<scala.concurrent.Future<Object>> results = new ArrayList<>();

        return new StreamObserver<Udfunction.Datum>() {
            @Override
            public void onNext(Udfunction.Datum datum) {
                // get Datum from request
                HandlerDatum handlerDatum = new HandlerDatum(
                        datum.getValue().toByteArray(),
                        Instant.ofEpochSecond(
                                datum.getWatermark().getWatermark().getSeconds(),
                                datum.getWatermark().getWatermark().getNanos()),
                        Instant.ofEpochSecond(
                                datum.getEventTime().getEventTime().getSeconds(),
                                datum.getEventTime().getEventTime().getNanos()));
                try {
                    if (!streamMap.containsKey(datum.getKey())) {

                        ReduceDatumStreamImpl reduceDatumStream = new ReduceDatumStreamImpl();
                        String workerId = datum.getKey() + datum.getEventTime();

                        ActorRef actorRef = actorSystem.actorOf(SimpleActor.props(
                                datum.getKey(),
                                md,
                                reduceHandler));

                        results.add(Patterns.ask(actorRef, reduceDatumStream, Integer.MAX_VALUE));
                        streamMap.put(datum.getKey(), reduceDatumStream);
                        actorList.add(actorRef);
                    }
                    streamMap.get(datum.getKey()).WriteMessage(handlerDatum);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    onError(e);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.log(Level.WARNING, "Encountered error in reduceFn", throwable);
                responseObserver.onError(throwable);
                responseObserver.onCompleted();
            }

            @Override
            public void onCompleted() {
                Udfunction.DatumList.Builder responseBuilder = Udfunction.DatumList.newBuilder();
                try {
                    for (Map.Entry<String, ReduceDatumStreamImpl> entry : streamMap.entrySet()) {
                        entry.getValue().WriteMessage(ReduceDatumStream.EOF);
                    }
                    Future<Iterable<Object>> finalFuture = buildDatumListResponse(
                            results,
                            actorSystem);

                    finalFuture.onComplete(new OnComplete<>() {
                        @Override
                        public void onComplete(
                                Throwable failure,
                                Iterable<Object> success) {

                            if (failure == null) {
                                success.forEach(ele -> {
                                    Udfunction.DatumList list = buildDatumListResponse((Message[]) ele);
                                    responseBuilder.addAllElements(list.getElementsList());
                                });
                                Udfunction.DatumList response = responseBuilder.build();
                                responseObserver.onNext(response);
                            } else {
                                logger.severe("error while getting output from actors - "
                                        + failure.getMessage());
                                responseObserver.onError(failure);
                            }
                            responseObserver.onCompleted();

                            // terminate all the actors
                            for (ActorRef actorRef: actorList) {
                                actorSystem.stop(actorRef);
                            }

                            logger.info("actors are terminated");
                        }
                    }, actorSystem.dispatcher());
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    onError(e);
                }
                logger.info("Actor system was terminated");
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

//    // shuts down the executor service which is used for reduce
//    public void shutDown() {
//        this.reduceTaskExecutor.shutdown();
//        try {
//            if (!reduceTaskExecutor.awaitTermination(SHUTDOWN_TIME, TimeUnit.SECONDS)) {
//                System.err.println("Reduce executor did not terminate in the specified time.");
//                List<Runnable> droppedTasks = reduceTaskExecutor.shutdownNow();
//                System.err.println("Reduce executor was abruptly shut down. " + droppedTasks.size()
//                        + " tasks will not be executed.");
//            } else {
//                System.err.println("Reduce executor was terminated.");
//            }
//        } catch (InterruptedException e) {
//            Thread.interrupted();
//            e.printStackTrace();
//        }
//    }

    private Future<Iterable<Object>> buildDatumListResponse(
            List<Future<Object>> results,
            ActorSystem actorSystem) {
        // wait for all the handlers to return
        Future<Iterable<Object>> finalFuture = Futures.sequence(results, actorSystem.dispatcher());
        return finalFuture;

    }

    private Udfunction.DatumList buildDatumListResponse(Message[] messages) {
        Udfunction.DatumList.Builder datumListBuilder = Udfunction.DatumList.newBuilder();
        for (Message message : messages) {
            Udfunction.Datum d = Udfunction.Datum.newBuilder()
                    .setKey(message.getKey())
                    .setValue(ByteString.copyFrom(message.getValue()))
                    .build();

            datumListBuilder.addElements(d);
        }
        return datumListBuilder.build();
    }
}
