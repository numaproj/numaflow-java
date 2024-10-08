package io.numaproj.numaflow.mapper;

import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.map.v1.MapOuterClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MapOutputStreamObserver implements StreamObserver<MapOuterClass.MapResponse> {
    List<MapOuterClass.MapResponse> mapResponses = new ArrayList<>();
    CompletableFuture<Void> done = new CompletableFuture<>();
    Integer responseCount;

    public MapOutputStreamObserver(Integer responseCount) {
        this.responseCount = responseCount;
    }

    @Override
    public void onNext(MapOuterClass.MapResponse mapResponse) {
        mapResponses.add(mapResponse);
        if (mapResponses.size() == responseCount) {
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

    public List<MapOuterClass.MapResponse> getMapResponses() {
        return mapResponses;
    }
}
