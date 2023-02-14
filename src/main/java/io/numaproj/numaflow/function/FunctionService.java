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
import io.numaproj.numaflow.function.mapt.MapTHandler;
import io.numaproj.numaflow.function.metadata.IntervalWindow;
import io.numaproj.numaflow.function.metadata.IntervalWindowImpl;
import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.metadata.MetadataImpl;
import io.numaproj.numaflow.function.reduce.GroupBy;
import io.numaproj.numaflow.function.reduce.ReduceSupervisorActor;
import io.numaproj.numaflow.function.reduce.ShutdownActor;
import io.numaproj.numaflow.function.v1.Udfunction;
import io.numaproj.numaflow.function.v1.Udfunction.EventTime;
import io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static io.numaproj.numaflow.function.Function.EOF;
import static io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc.getMapFnMethod;
import static io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc.getReduceFnMethod;

@Slf4j
@NoArgsConstructor
public class FunctionService extends UserDefinedFunctionGrpc.UserDefinedFunctionImplBase {

    public static final ActorSystem actorSystem = ActorSystem.create("reduce");

    private MapHandler mapHandler;
    private MapTHandler mapTHandler;
    private Class<? extends GroupBy> groupBy;

    public void setMapHandler(MapHandler mapHandler) {
        this.mapHandler = mapHandler;
    }

    public void setMapTHandler(MapTHandler mapTHandler) {
        this.mapTHandler = mapTHandler;
    }

    public void setReduceHandler(Class<? extends GroupBy> groupBy) {
        this.groupBy = groupBy;
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
                        request.getEventTime().getEventTime().getNanos())
        );

        // process Datum
        Message[] messages = mapHandler.HandleDo(key, handlerDatum);

        // set response
        responseObserver.onNext(buildDatumListResponse(messages));
        responseObserver.onCompleted();
    }

    @Override
    public void mapTFn(
            Udfunction.Datum request,
            StreamObserver<Udfunction.DatumList> responseObserver) {

        if (this.mapTHandler == null) {
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
                        request.getEventTime().getEventTime().getNanos())
        );

        // process Datum
        MessageT[] messageTs = mapTHandler.HandleDo(key, handlerDatum);

        // set response
        responseObserver.onNext(buildDatumListResponse(messageTs));
        responseObserver.onCompleted();
    }

    /**
     * Streams input data to reduceFn and returns the result.
     */
    @Override
    public StreamObserver<Udfunction.Datum> reduceFn(final StreamObserver<Udfunction.DatumList> responseObserver) {

        if (this.groupBy == null) {
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

        CompletableFuture<Void> failureFuture = new CompletableFuture<>();

        // create a shutdown actor that listens to exceptions.
        ActorRef shutdownActorRef = actorSystem.
                actorOf(ShutdownActor.props(responseObserver, failureFuture));

        handleFailure(failureFuture);
        /*
            create a supervisor actor which assign the tasks to child actors.
            we create a child actor for every key in a window.
        */
        ActorRef parentActorRef = actorSystem
                .actorOf(ReduceSupervisorActor.props(groupBy, md, shutdownActorRef));


        return new StreamObserver<Udfunction.Datum>() {
            @Override
            public void onNext(Udfunction.Datum datum) {
                // send the message to parent actor, which takes care of distribution.
                if (!parentActorRef.isTerminated()) {
                    parentActorRef.tell(datum, parentActorRef);
                } else {
                    responseObserver.onError(new Throwable("Supervisor actor was terminated"));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("Error from the client - {}" , throwable.getMessage());
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {

                // Ask the parent to return the list of futures returned by child actors.
                Future<Object> resultFuture = Patterns.ask(parentActorRef, EOF, Integer.MAX_VALUE);

                List<Future<Object>> udfResultFutures;
                try {
                    udfResultFutures = (List<Future<Object>>) Await.result(
                            resultFuture,
                            Duration.Inf());
                } catch (TimeoutException | InterruptedException e) {
                    responseObserver.onError(e);
                    return;
                }

                extractResult(udfResultFutures, responseObserver, parentActorRef);

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

    private Udfunction.DatumList buildDatumListResponse(Message[] messages) {
        Udfunction.DatumList.Builder datumListBuilder = Udfunction.DatumList.newBuilder();
        Arrays.stream(messages).forEach(message -> {
            datumListBuilder.addElements(Udfunction.Datum.newBuilder()
                    .setKey(message.getKey())
                    .setValue(ByteString.copyFrom(message.getValue()))
                    .build());
        });
        return datumListBuilder.build();
    }

    private Udfunction.DatumList buildDatumListResponse(MessageT[] messageTs) {
        Udfunction.DatumList.Builder datumListBuilder = Udfunction.DatumList.newBuilder();
        Arrays.stream(messageTs).forEach(messageT -> {
            datumListBuilder.addElements(Udfunction.Datum.newBuilder()
                    .setEventTime(EventTime.newBuilder().setEventTime
                            (com.google.protobuf.Timestamp.newBuilder()
                                    .setSeconds(messageT.getEventTime().getEpochSecond())
                                    .setNanos(messageT.getEventTime().getNano()))
                    )
                    .setKey(messageT.getKey())
                    .setValue(ByteString.copyFrom(messageT.getValue()))
                    .build());
        });
        return datumListBuilder.build();
    }

    /*
        extracts and returns the result to the observer.
     */
    private void extractResult(List<Future<Object>> futureList, StreamObserver<Udfunction.DatumList> responseObserver, ActorRef supervisorActorRef) {
        Udfunction.DatumList.Builder responseBuilder = Udfunction.DatumList.newBuilder();

        // build the response when its completed.
        Futures
                .sequence(futureList, actorSystem.dispatcher())
                .onComplete(new OnComplete<>() {
                    @Override
                    public void onComplete(
                            Throwable failure,
                            Iterable<Object> success) {

                        // if there are any failures indicate it to the observer.
                        if (failure != null) {
                            log.error("Error while getting output from actors - {}"
                                    , failure.getMessage());

                            responseObserver.onError(failure);
                            return;
                        }

                        success.forEach(ele -> {
                            Udfunction.DatumList list = buildDatumListResponse((Message[]) ele);
                            responseBuilder.addAllElements(list.getElementsList());
                        });
                        Udfunction.DatumList response = responseBuilder.build();

                        responseObserver.onNext(response);
                        responseObserver.onCompleted();

                        /*
                            once the result is returned we can stop the supervisor actor.
                            stopping the supervisor will stop all its child actors.
                            we should  explicitly stop the actors for it to be garbage collected.
                        */
                        actorSystem.stop(supervisorActorRef);
                    }
                }, actorSystem.dispatcher());
    }

    // log the exception and exit if there are any uncaught exceptions.
    private void handleFailure(CompletableFuture<Void> failureFuture) {
        new Thread(() -> {
            try {
                failureFuture.get();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }).start();
    }
}
