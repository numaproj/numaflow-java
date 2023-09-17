package io.numaproj.numaflow.mapstreamer;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.mapstream.v1.MapStreamGrpc;
import io.numaproj.numaflow.mapstream.v1.Mapstream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;

import static io.numaproj.numaflow.mapstream.v1.MapStreamGrpc.getMapStreamFnMethod;


@Slf4j
@AllArgsConstructor
class Service extends MapStreamGrpc.MapStreamImplBase {

    private final MapStreamer mapStreamer;

    /**
     * Applies a map stream function to each request.
     */
    @Override
    public void mapStreamFn(
            Mapstream.MapStreamRequest request,
            StreamObserver<Mapstream.MapStreamResponse> responseObserver) {

        if (this.mapStreamer == null) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(
                    getMapStreamFnMethod(),
                    responseObserver);
            return;
        }

        HandlerDatum handlerDatum = new HandlerDatum(
                request.getValue().toByteArray(),
                Instant.ofEpochSecond(
                        request.getWatermark().getSeconds(),
                        request.getWatermark().getNanos()),
                Instant.ofEpochSecond(
                        request.getEventTime().getSeconds(),
                        request.getEventTime().getNanos())
        );

        // process Datum
        this.mapStreamer.processMessage(request
                .getKeysList()
                .toArray(new String[0]), handlerDatum, new OutputObserverImpl(responseObserver));

        responseObserver.onCompleted();
    }

    /**
     * IsReady is the heartbeat endpoint for gRPC.
     */
    @Override
    public void isReady(Empty request, StreamObserver<Mapstream.ReadyResponse> responseObserver) {
        responseObserver.onNext(Mapstream.ReadyResponse.newBuilder().setReady(true).build());
        responseObserver.onCompleted();
    }

}
