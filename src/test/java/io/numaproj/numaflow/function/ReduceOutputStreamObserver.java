package io.numaproj.numaflow.function;

import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.function.v1.Udfunction;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class ReduceOutputStreamObserver implements StreamObserver<Udfunction.DatumResponseList> {
    public AtomicReference<Boolean> completed = new AtomicReference<>(false);
    public AtomicReference<Udfunction.DatumResponseList> resultDatum = new AtomicReference<>(
            Udfunction.DatumResponseList.newBuilder().build());
    public Throwable t;

    @Override
    public void onNext(Udfunction.DatumResponseList datum) {
        resultDatum.set(resultDatum
                .get()
                .toBuilder()
                .addAllElements(datum.getElementsList())
                .build());
    }

    @Override
    public void onError(Throwable throwable) {
        t = throwable;
    }

    @Override
    public void onCompleted() {
        log.info("on completed executed");
        this.completed.set(true);
    }
}
