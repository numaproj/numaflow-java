package io.numaproj.numaflow.reducer;

import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class ReduceOutputStreamObserver implements StreamObserver<ReduceOuterClass.ReduceResponse> {
    public AtomicReference<Boolean> completed = new AtomicReference<>(false);
    public AtomicReference<ReduceOuterClass.ReduceResponse> resultDatum = new AtomicReference<>(
            ReduceOuterClass.ReduceResponse.newBuilder().build());
    public Throwable t;

    @Override
    public void onNext(ReduceOuterClass.ReduceResponse datum) {
        resultDatum.set(resultDatum
                .get()
                .toBuilder()
                .addAllResults(datum.getResultsList())
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
