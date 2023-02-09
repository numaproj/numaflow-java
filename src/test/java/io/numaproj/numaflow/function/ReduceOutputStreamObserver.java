package io.numaproj.numaflow.function;

import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.function.v1.Udfunction;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReduceOutputStreamObserver implements StreamObserver<Udfunction.DatumList> {
    public Udfunction.DatumList resultDatum;
    public Throwable t;

    @Override
    public void onNext(Udfunction.DatumList datum) {
        resultDatum = datum;
    }

    @Override
    public void onError(Throwable throwable) {
        t = throwable;
        notifyAll();
    }

    @Override
    public void onCompleted() {
        log.info("on completed executed");
    }
}
