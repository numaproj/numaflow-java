package io.numaproj.numaflow.accumulator;

import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.accumulator.v1.AccumulatorOuterClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class AccumulatorStreamObserver implements StreamObserver<AccumulatorOuterClass.AccumulatorResponse> {
    List<AccumulatorOuterClass.AccumulatorResponse> responses = new ArrayList<>();
    CompletableFuture<Void> done = new CompletableFuture<>();
    Integer responseCount;

    public AccumulatorStreamObserver(Integer responseCount) {
        this.responseCount = responseCount;
    }

    @Override
    public void onNext(AccumulatorOuterClass.AccumulatorResponse response) {
        responses.add(response);
        if (responses.size() == responseCount) {
            done.complete(null);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        done.completeExceptionally(throwable);
    }

    @Override
    public void onCompleted() {
        done.complete(null);
    }

    public List<AccumulatorOuterClass.AccumulatorResponse> getResponses() {
        return responses;
    }
}
