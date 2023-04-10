package io.numaproj.numaflow.function;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AllDeadLetters;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.function.map.MapHandler;
import io.numaproj.numaflow.function.mapt.MapTHandler;
import io.numaproj.numaflow.function.metadata.IntervalWindow;
import io.numaproj.numaflow.function.metadata.IntervalWindowImpl;
import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.metadata.MetadataImpl;
import io.numaproj.numaflow.function.reduce.ReduceHandler;
import io.numaproj.numaflow.function.reduce.ReduceSupervisorActor;
import io.numaproj.numaflow.function.reduce.ReducerFactory;
import io.numaproj.numaflow.function.reduce.ShutdownActor;
import io.numaproj.numaflow.function.v1.Udfunction;
import io.numaproj.numaflow.function.v1.Udfunction.EventTime;
import io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.numaproj.numaflow.function.Function.EOF;
import static io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc.getMapFnMethod;
import static io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc.getReduceFnMethod;

@Slf4j
@NoArgsConstructor
public class FunctionService extends UserDefinedFunctionGrpc.UserDefinedFunctionImplBase {

    public static final ActorSystem actorSystem = ActorSystem.create("reduce");

    private MapHandler mapHandler;
    private MapTHandler mapTHandler;
    private ReducerFactory<? extends ReduceHandler> reducerFactory;

    public void setMapHandler(MapHandler mapHandler) {
        this.mapHandler = mapHandler;
    }

    public void setMapTHandler(MapTHandler mapTHandler) {
        this.mapTHandler = mapTHandler;
    }

    public void setReduceHandler(ReducerFactory<? extends ReduceHandler> reducerFactory) {
        this.reducerFactory = reducerFactory;
    }

    /**
     * Applies a function to each datum element.
     */
    @Override
    public void mapFn(
            Udfunction.DatumRequest request,
            StreamObserver<Udfunction.DatumResponseList> responseObserver) {
        if (this.mapHandler == null) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(
                    getMapFnMethod(),
                    responseObserver);
            return;
        }

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
        MessageList messageList = mapHandler.processMessage(request
                .getKeysList()
                .toArray(new String[0]), handlerDatum);

        // set response
        responseObserver.onNext(buildDatumListResponse(messageList));
        responseObserver.onCompleted();
    }

    @Override
    public void mapTFn(
            Udfunction.DatumRequest request,
            StreamObserver<Udfunction.DatumResponseList> responseObserver) {

        if (this.mapTHandler == null) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(
                    getMapFnMethod(),
                    responseObserver);
            return;
        }

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
        MessageTList messageTList = mapTHandler.processMessage(request
                .getKeysList()
                .toArray(new String[0]), handlerDatum);

        // set response
        responseObserver.onNext(buildDatumListResponse(messageTList));
        responseObserver.onCompleted();
    }

    /**
     * Streams input data to reduceFn and returns the result.
     */
    @Override
    public StreamObserver<Udfunction.DatumRequest> reduceFn(final StreamObserver<Udfunction.DatumResponseList> responseObserver) {

        if (this.reducerFactory == null) {
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

        // subscribe for dead letters
        actorSystem.getEventStream().subscribe(shutdownActorRef, AllDeadLetters.class);

        handleFailure(failureFuture);
        /*
            create a supervisor actor which assign the tasks to child actors.
            we create a child actor for every unique set of keys in a window
        */
        ActorRef supervisorActor = actorSystem
                .actorOf(ReduceSupervisorActor.props(
                        reducerFactory,
                        md,
                        shutdownActorRef,
                        responseObserver));


        return new StreamObserver<Udfunction.DatumRequest>() {
            @Override
            public void onNext(Udfunction.DatumRequest datum) {
                // send the message to parent actor, which takes care of distribution.
                if (!supervisorActor.isTerminated()) {
                    supervisorActor.tell(datum, supervisorActor);
                } else {
                    responseObserver.onError(new Throwable("Supervisor actor was terminated"));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("Error from the client - {}", throwable.getMessage());
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                // indicate the end of input to the supervisor
                supervisorActor.tell(EOF, ActorRef.noSender());

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

    private Udfunction.DatumResponseList buildDatumListResponse(MessageList messageList) {
        Udfunction.DatumResponseList.Builder datumListBuilder = Udfunction.DatumResponseList.newBuilder();
        messageList.getMessages().forEach(message -> {
            datumListBuilder.addElements(Udfunction.DatumResponse.newBuilder()
                    .setValue(ByteString.copyFrom(message.getValue()))
                    .addAllKeys(message.getKeys() == null? new ArrayList<>(): List.of(message.getKeys()))
                    .addAllTags(message.getTags() == null? new ArrayList<>(): List.of(message.getTags()))
                    .build());
        });
        return datumListBuilder.build();
    }

    private Udfunction.DatumResponseList buildDatumListResponse(MessageTList messageTList) {
        Udfunction.DatumResponseList.Builder datumListBuilder = Udfunction.DatumResponseList.newBuilder();
        messageTList.getMessages().forEach(messageT -> {
            datumListBuilder.addElements(Udfunction.DatumResponse.newBuilder()
                    .setEventTime(EventTime.newBuilder().setEventTime
                            (com.google.protobuf.Timestamp.newBuilder()
                                    .setSeconds(messageT.getEventTime().getEpochSecond())
                                    .setNanos(messageT.getEventTime().getNano()))
                    )
                    .addAllKeys(messageT.getKeys() == null? new ArrayList<>(): List.of(messageT.getKeys()))
                    .addAllTags(messageT.getTags() == null? new ArrayList<>(): List.of(messageT.getTags()))
                    .setValue(ByteString.copyFrom(messageT.getValue()))
                    .build());
        });
        return datumListBuilder.build();
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
