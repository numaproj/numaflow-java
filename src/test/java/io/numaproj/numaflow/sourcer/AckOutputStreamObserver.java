package io.numaproj.numaflow.sourcer;


import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.source.v1.SourceOuterClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class AckOutputStreamObserver implements StreamObserver<SourceOuterClass.AckResponse> {
    private final List<SourceOuterClass.AckResponse> ackResponses = new ArrayList<>();
    public AtomicReference<Boolean> completed = new AtomicReference<>(false);
    public Throwable t;

    public List<SourceOuterClass.AckResponse> getSinkResponse() {
        return ackResponses;
    }

    @Override
    public void onNext(SourceOuterClass.AckResponse datum) {
        ackResponses.add(datum);
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
