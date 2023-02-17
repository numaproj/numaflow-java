package io.numaproj.numaflow.function;

import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.function.v1.Udfunction;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class ReduceOutputStreamObserver implements StreamObserver<Udfunction.DatumList> {
    public AtomicReference<Udfunction.DatumList> resultDatum = new AtomicReference<>();
    public Throwable t;

    @Override
    public void onNext(Udfunction.DatumList datum) {
        resultDatum.set(datum);
    }

    @Override
    public void onError(Throwable throwable) {
        t = throwable;
    }

    @Override
    public void onCompleted() {
        log.info("on completed executed");
    }
}
