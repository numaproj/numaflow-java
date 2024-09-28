package io.numaproj.numaflow.sourcer;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.source.v1.SourceGrpc;
import io.numaproj.numaflow.source.v1.SourceOuterClass;

import java.time.Duration;
import java.util.List;

import static io.numaproj.numaflow.source.v1.SourceGrpc.getPendingFnMethod;


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
     * @param responseObserver the response observer
     */
    @Override
    public StreamObserver<SourceOuterClass.ReadRequest> readFn(final StreamObserver<SourceOuterClass.ReadResponse> responseObserver) {
        return new StreamObserver<>() {
            private boolean handshakeDone = false;

            @Override
            public void onNext(SourceOuterClass.ReadRequest request) {
                // make sure that the handshake is done before processing the read requests
                if (!handshakeDone) {
                    if (!request.hasHandshake() || !request.getHandshake().getSot()) {
                        responseObserver.onError(Status.INVALID_ARGUMENT
                                .withDescription("Handshake request not received")
                                .asException());
                        return;
                    }
                    responseObserver.onNext(SourceOuterClass.ReadResponse.newBuilder()
                            .setHandshake(request.getHandshake())
                            .build());
                    handshakeDone = true;
                    return;
                }

                ReadRequestImpl readRequest = new ReadRequestImpl(
                        request.getRequest().getNumRecords(),
                        Duration.ofMillis(request.getRequest().getTimeoutInMs()));

                // Create an observer to write the response back to the client
                OutputObserverImpl outputObserver = new OutputObserverImpl(responseObserver);

                // invoke the sourcer's read method
                sourcer.read(readRequest, outputObserver);

                // once the read is done, send an EOT message to indicate the client
                // that the end of batch has been reached
                SourceOuterClass.ReadResponse.Status status = SourceOuterClass.ReadResponse.Status
                        .newBuilder()
                        .setEot(true)
                        .setCode(SourceOuterClass.ReadResponse.Status.Code.SUCCESS)
                        .build();

                SourceOuterClass.ReadResponse response = SourceOuterClass.ReadResponse.newBuilder()
                        .setStatus(status)
                        .build();

                responseObserver.onNext(response);
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(Status.UNKNOWN
                        .withDescription(t.getMessage())
                        .asException());
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    /**
     * ackFn is the endpoint for acknowledging data from the sourcer.
     *
     * @param responseObserver the response observer
     */
    @Override
    public StreamObserver<SourceOuterClass.AckRequest> ackFn(final StreamObserver<SourceOuterClass.AckResponse> responseObserver) {
        return new StreamObserver<>() {
            private boolean handshakeDone = false;

            @Override
            public void onNext(SourceOuterClass.AckRequest request) {
                // make sure that the handshake is done before processing the ack requests
                if (!handshakeDone) {
                    if (!request.hasHandshake() || !request.getHandshake().getSot()) {
                        responseObserver.onError(Status.INVALID_ARGUMENT
                                .withDescription("Handshake request not received")
                                .asException());
                        return;
                    }
                    responseObserver.onNext(SourceOuterClass.AckResponse.newBuilder()
                            .setHandshake(request.getHandshake())
                            .build());
                    handshakeDone = true;
                    return;
                }

                SourceOuterClass.Offset offset = request.getRequest().getOffset();

                AckRequestImpl ackRequest = new AckRequestImpl(new Offset(
                        offset.getOffset().toByteArray(),
                        offset.getPartitionId()));

                // invoke the sourcer's ack method

                sourcer.ack(ackRequest);

                // send an ack response to the client after acking the message
                SourceOuterClass.AckResponse response = SourceOuterClass.AckResponse
                        .newBuilder()
                        .setResult(SourceOuterClass.AckResponse.Result.newBuilder().setSuccess(
                                Empty.newBuilder().build()))
                        .build();

                responseObserver.onNext(response);
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
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

    @Override
    public void partitionsFn(
            Empty request,
            StreamObserver<SourceOuterClass.PartitionsResponse> responseObserver) {

        if (this.sourcer == null) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(
                    getPendingFnMethod(),
                    responseObserver);
            return;
        }

        List<Integer> partitions = this.sourcer.getPartitions();
        responseObserver.onNext(SourceOuterClass.PartitionsResponse.newBuilder()
                .setResult(
                        SourceOuterClass.PartitionsResponse.Result.newBuilder()
                                .addAllPartitions(partitions)).
                build());
        responseObserver.onCompleted();
    }
}
