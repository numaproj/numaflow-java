package io.numaproj.numaflow.function;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.DeadLetter;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.function.map.MapHandler;
import io.numaproj.numaflow.function.mapt.MapTHandler;
import io.numaproj.numaflow.function.metadata.IntervalWindow;
import io.numaproj.numaflow.function.metadata.IntervalWindowImpl;
import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.metadata.MetadataImpl;
import io.numaproj.numaflow.function.reduce.ReduceSupervisorActor;
import io.numaproj.numaflow.function.reduce.ReduceHandler;
import io.numaproj.numaflow.function.reduce.ReducerFactory;
import io.numaproj.numaflow.function.reduce.ShutdownActor;
import io.numaproj.numaflow.function.v1.Udfunction;
import io.numaproj.numaflow.function.v1.Udfunction.EventTime;
import io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Arrays;
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
        Message[] messages = mapHandler.processMessage(key, handlerDatum);

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
        MessageT[] messageTs = mapTHandler.processMessage(key, handlerDatum);

        // set response
        responseObserver.onNext(buildDatumListResponse(messageTs));
        responseObserver.onCompleted();
    }

    /**
     * Streams input data to reduceFn and returns the result.
     */
    @Override
    public StreamObserver<Udfunction.Datum> reduceFn(final StreamObserver<Udfunction.DatumList> responseObserver) {

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


        actorSystem.getEventStream().subscribe(shutdownActorRef, DeadLetter.class);

        handleFailure(failureFuture);
        /*
            create a supervisor actor which assign the tasks to child actors.
            we create a child actor for every key in a window.
        */
        ActorRef supervisorActor = actorSystem
                .actorOf(ReduceSupervisorActor.props(reducerFactory, md, shutdownActorRef, responseObserver));


        return new StreamObserver<Udfunction.Datum>() {
            @Override
            public void onNext(Udfunction.Datum datum) {
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
