package io.numaproj.numaflow.function;

import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.function.v1.Udfunction;

public class ReduceOutputStreamObserver implements StreamObserver<Udfunction.DatumList> {
    private Udfunction.DatumList resultDatum;

    public Udfunction.DatumList getResultDatum() {
        return resultDatum;
    }

    @Override
    public void onNext(Udfunction.DatumList datum) {
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
