package io.numaproj.numaflow.sourcer;


import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.source.v1.SourceOuterClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class ReadOutputStreamObserver implements StreamObserver<SourceOuterClass.ReadResponse> {
    private final List<SourceOuterClass.ReadResponse> readResponses = new ArrayList<>();
    public AtomicReference<Boolean> completed = new AtomicReference<>(false);
    public Throwable t;

    public List<SourceOuterClass.ReadResponse> getSinkResponse() {
        return readResponses;
    }

    @Override
    public void onNext(SourceOuterClass.ReadResponse datum) {
        readResponses.add(datum);
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
