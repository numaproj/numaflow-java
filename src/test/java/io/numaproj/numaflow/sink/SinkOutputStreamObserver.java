//package io.numaproj.numaflow.sink;
//
//import io.grpc.stub.StreamObserver;
//import io.numaproj.numaflow.sink.v1.Udsink;
//
//import java.util.concurrent.atomic.AtomicReference;
//
//public class SinkOutputStreamObserver implements StreamObserver<Udsink.ResponseList> {
//    private Udsink.ResponseList resultDatum;
//    public AtomicReference<Boolean> completed = new AtomicReference<>(false);
//
//    public Throwable t;
//
//    public Udsink.ResponseList getResultDatum() {
//        return resultDatum;
//    }
//
//    @Override
//    public void onNext(Udsink.ResponseList datum) {
//        resultDatum = datum;
//    }
//
//    @Override
//    public void onError(Throwable throwable) {
//        t = throwable;
//    }
//
//    @Override
//    public void onCompleted() {
//        this.completed.set(true);
//    }
//}
