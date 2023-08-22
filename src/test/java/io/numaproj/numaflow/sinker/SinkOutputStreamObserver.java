package io.numaproj.numaflow.sinker;


import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sink.v1.SinkOuterClass;

import java.util.concurrent.atomic.AtomicReference;

public class SinkOutputStreamObserver implements StreamObserver<SinkOuterClass.SinkResponse> {
    private SinkOuterClass.SinkResponse sinkResponse;
    public AtomicReference<Boolean> completed = new AtomicReference<>(false);

    public Throwable t;

    public SinkOuterClass.SinkResponse getSinkResponse() {
        return sinkResponse;
    }

    @Override
    public void onNext(SinkOuterClass.SinkResponse datum) {
        sinkResponse = datum;
    }

    @Override
    public void onError(Throwable throwable) {
        t = throwable;
    }

    @Override
    public void onCompleted() {
        this.completed.set(true);
    }
}
