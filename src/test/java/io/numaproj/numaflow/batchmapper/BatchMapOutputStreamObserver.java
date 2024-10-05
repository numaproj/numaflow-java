package io.numaproj.numaflow.batchmapper;

import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.batchmap.v1.Batchmap;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class BatchMapOutputStreamObserver implements StreamObserver<Batchmap.BatchMapResponse> {
    public AtomicReference<Boolean> completed = new AtomicReference<>(false);
    public AtomicReference<List<Batchmap.BatchMapResponse>> resultDatum = new AtomicReference<>(
            new ArrayList<>());
    public Throwable t;

    @Override
    public void onNext(Batchmap.BatchMapResponse batchMapResponse) {
        List<Batchmap.BatchMapResponse> receivedResponses = resultDatum.get();
        receivedResponses.add(batchMapResponse);
        resultDatum.set(receivedResponses);
        log.info(
                "Received BatchMapResponse with id {} and message count {}",
                batchMapResponse.getId(),
                batchMapResponse.getResultsCount());
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
