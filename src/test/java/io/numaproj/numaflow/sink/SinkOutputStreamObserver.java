package io.numaproj.numaflow.sink;

import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sink.v1.Udsink;

public class SinkOutputStreamObserver implements StreamObserver<Udsink.ResponseList> {
    private Udsink.ResponseList resultDatum;

    public Udsink.ResponseList getResultDatum() {
        return resultDatum;
    }

    @Override
    public void onNext(Udsink.ResponseList datum) {
        resultDatum = datum;
    }

    @Override
    public void onError(Throwable throwable) {
        throwable.printStackTrace();
    }

    @Override
    public void onCompleted() {
    }
}
