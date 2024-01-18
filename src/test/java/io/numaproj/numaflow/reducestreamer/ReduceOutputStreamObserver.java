package io.numaproj.numaflow.reducestreamer;

import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is a dummy implementation of reduce output stream observer for testing purpose.
 */
@Slf4j
public class ReduceOutputStreamObserver implements StreamObserver<ReduceOuterClass.ReduceResponse> {
    public AtomicReference<Boolean> completed = new AtomicReference<>(false);
    public AtomicReference<List<ReduceOuterClass.ReduceResponse>> resultDatum = new AtomicReference<>(
            new ArrayList<>());
    public Throwable t;

    @Override
    public synchronized void onNext(ReduceOuterClass.ReduceResponse response) {
        List<ReduceOuterClass.ReduceResponse> receivedResponses = resultDatum.get();
        receivedResponses.add(response);
        resultDatum.set(receivedResponses);
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
