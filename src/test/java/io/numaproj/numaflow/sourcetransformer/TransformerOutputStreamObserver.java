package io.numaproj.numaflow.sourcetransformer;

import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.sourcetransformer.v1.Sourcetransformer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class TransformerOutputStreamObserver implements StreamObserver<Sourcetransformer.SourceTransformResponse> {
    List<Sourcetransformer.SourceTransformResponse> responses = new ArrayList<>();
    CompletableFuture<Void> done = new CompletableFuture<>();
    Integer responseCount;

    public TransformerOutputStreamObserver(Integer responseCount) {
        this.responseCount = responseCount;
    }

    @Override
    public void onNext(Sourcetransformer.SourceTransformResponse mapResponse) {
        responses.add(mapResponse);
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

    public List<Sourcetransformer.SourceTransformResponse> getResponses() {
        return responses;
    }
}
