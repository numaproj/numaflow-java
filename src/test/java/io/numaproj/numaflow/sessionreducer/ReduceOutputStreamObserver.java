package io.numaproj.numaflow.sessionreducer;

import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is a dummy implementation of reduce output stream observer for testing purpose.
 */
@Slf4j
public class ReduceOutputStreamObserver implements StreamObserver<Sessionreduce.SessionReduceResponse> {
    public AtomicReference<Boolean> completed = new AtomicReference<>(false);
    public AtomicReference<List<Sessionreduce.SessionReduceResponse>> resultDatum = new AtomicReference<>(
            new ArrayList<>());
    public Throwable t;

    @Override
    public void onNext(Sessionreduce.SessionReduceResponse response) {
        List<Sessionreduce.SessionReduceResponse> receivedResponses = resultDatum.get();
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
