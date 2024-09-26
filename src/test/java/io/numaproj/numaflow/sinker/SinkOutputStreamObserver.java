package io.numaproj.numaflow.sinker;


import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sink.v1.SinkOuterClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class SinkOutputStreamObserver implements StreamObserver<SinkOuterClass.SinkResponse> {
    private final List<SinkOuterClass.SinkResponse> sinkResponses = new ArrayList<>();
    public AtomicReference<Boolean> completed = new AtomicReference<>(false);
    public Throwable t;

    public List<SinkOuterClass.SinkResponse> getSinkResponse() {
        return sinkResponses;
    }

    @Override
    public void onNext(SinkOuterClass.SinkResponse datum) {
        sinkResponses.add(datum);
    }

    @Override
    public void onError(Throwable throwable) {
        t = throwable;
        this.completed.set(true);
    }

    @Override
    public void onCompleted() {
        this.completed.set(true);
    }
}
