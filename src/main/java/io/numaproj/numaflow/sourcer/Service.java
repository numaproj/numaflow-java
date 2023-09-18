package io.numaproj.numaflow.sourcer;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.source.v1.SourceGrpc;
import io.numaproj.numaflow.source.v1.SourceOuterClass;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.numaproj.numaflow.source.v1.SourceGrpc.getAckFnMethod;
import static io.numaproj.numaflow.source.v1.SourceGrpc.getPendingFnMethod;
import static io.numaproj.numaflow.source.v1.SourceGrpc.getReadFnMethod;


/**
 * Implementation of the gRPC service for the sourcer.
 */
class Service extends SourceGrpc.SourceImplBase {
    private final Sourcer sourcer;

    public Service(Sourcer sourcer) {
        this.sourcer = sourcer;
    }

    /**
     * readFn is the endpoint for reading data from the sourcer.
     *
     * @param request the request
     * @param responseObserver the response observer
     */
    @Override
    public void readFn(
            SourceOuterClass.ReadRequest request,
            StreamObserver<SourceOuterClass.ReadResponse> responseObserver) {
        if (this.sourcer == null) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(
                    getReadFnMethod(),
                    responseObserver);
            return;
        }
        OutputObserverImpl outputObserver = new OutputObserverImpl(responseObserver);
        ReadRequestImpl readRequest = new ReadRequestImpl(
                request.getRequest().getNumRecords(),
                Duration.ofMillis(request.getRequest().getTimeoutInMs()));
        this.sourcer.read(readRequest, outputObserver);
        responseObserver.onCompleted();
    }

    /**
     * ackFn is the endpoint for acknowledging data from the sourcer.
     *
     * @param request the request
     * @param responseObserver the response observer
     */
    @Override
    public void ackFn(
            SourceOuterClass.AckRequest request,
            StreamObserver<SourceOuterClass.AckResponse> responseObserver) {

        if (this.sourcer == null) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(
                    getAckFnMethod(),
                    responseObserver);
            return;
        }

        List<Offset> offsets = new ArrayList<>(request.getRequest().getOffsetsCount());
        for (SourceOuterClass.Offset offset : request.getRequest().getOffsetsList()) {
            offsets.add(new Offset(offset.getOffset().toByteArray(), offset.getPartitionId()));
        }

        AckRequestImpl ackRequest = new AckRequestImpl(offsets);
        this.sourcer.ack(ackRequest);

        responseObserver.onNext(SourceOuterClass.AckResponse
                .newBuilder()
                .setResult(SourceOuterClass.AckResponse.Result.newBuilder().setSuccess(
                        Empty.newBuilder().build()))
                .build());
        responseObserver.onCompleted();
    }

    /**
     * pendingFn is the endpoint for getting the number of pending messages from the sourcer.
     *
     * @param request the request
     * @param responseObserver the response observer
     */
    @Override
    public void pendingFn(
            Empty request,
            StreamObserver<SourceOuterClass.PendingResponse> responseObserver) {

        if (this.sourcer == null) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(
                    getPendingFnMethod(),
                    responseObserver);
            return;
        }

        responseObserver.onNext(SourceOuterClass.PendingResponse.newBuilder().setResult(
                SourceOuterClass.PendingResponse.Result
                        .newBuilder()
                        .setCount(this.sourcer.getPending())
                        .build()).build());
        responseObserver.onCompleted();
    }

    /**
     * isReady is the endpoint for checking if the sourcer is ready.
     *
     * @param request the request
     * @param responseObserver the response observer
     */
    @Override
    public void isReady(
            Empty request,
            StreamObserver<SourceOuterClass.ReadyResponse> responseObserver) {

        responseObserver.onNext(SourceOuterClass.ReadyResponse.newBuilder().setReady(true).build());
        responseObserver.onCompleted();
    }
}
