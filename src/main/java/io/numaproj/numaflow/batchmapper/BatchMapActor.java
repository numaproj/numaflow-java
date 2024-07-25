package io.numaproj.numaflow.batchmapper;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.batchmap.v1.Batchmap;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
//@AllArgsConstructor
public class BatchMapActor extends AbstractActor {
    private final BatchMapper batchMapper;
    private final ActorRef shutdownActor;
    private final StreamObserver<Batchmap.BatchMapResponse> responseObserver;

    public BatchMapActor(
            BatchMapper batchMapper,
            ActorRef shutdownActor,
            StreamObserver<Batchmap.BatchMapResponse> responseObserver) {
        this.batchMapper = batchMapper;
        this.shutdownActor = shutdownActor;
        this.responseObserver = responseObserver;
    }

    public static Props props(
            BatchMapper batchMapHandler,
            ActorRef shutdownActor,
            StreamObserver<Batchmap.BatchMapResponse> responseObserver) {
        return Props.create(
                BatchMapActor.class,
                batchMapHandler,
                shutdownActor,
                responseObserver);
    }

    @Override
    public void preRestart(Throwable reason, Optional<Object> message) {
        log.debug("supervisor pre restart was executed");
        shutdownActor.tell(reason, ActorRef.noSender());
        Service.batchMapActorSystem.stop(getSelf());
    }

    @Override
    public void postStop() {
        log.debug("post stop of supervisor executed - {}", getSelf().toString());
        shutdownActor.tell(Constants.SUCCESS, ActorRef.noSender());
    }


    @Override
    public Receive createReceive() {
        return ReceiveBuilder
                .create()
                .match(DatumIteratorImpl.class, this::invokeHandler)
                .build();
    }

    private void invokeHandler(DatumIteratorImpl iterator) {
        BatchResponses resultMessages = this.batchMapper.processMessage(iterator);

        log.debug(
                "Finished the call Result size is :{} and iterator count is :{}",
                resultMessages.getItems().size(),
                iterator.getCount());
        // Crash if the number of responses from the users don't match the input requests ignoring the EOF message
        if (resultMessages.getItems().size() != iterator.getCount() - 1) {
            throw new RuntimeException("Number of results did not match");
        }

        resultMessages.getItems().forEach(message -> {
            List<Batchmap.BatchMapResponse.Result> batchMapResponseResult = new ArrayList<>();
            message.getItems().forEach(res -> {
                batchMapResponseResult.add(
                        Batchmap.BatchMapResponse.Result
                                .newBuilder()
                                .setValue(res.getValue()
                                        == null ? ByteString.EMPTY : ByteString.copyFrom(
                                        res.getValue()))
                                .addAllKeys(res.getKeys()
                                        == null ? new ArrayList<>() : List.of(res.getKeys()))
                                .addAllTags(res.getTags()
                                        == null ? new ArrayList<>() : List.of(res.getTags()))
                                .build()
                );
            });
            Batchmap.BatchMapResponse singleRequestResponse = Batchmap.BatchMapResponse
                    .newBuilder()
                    .setId(message.getId())
                    .addAllResults(batchMapResponseResult)
                    .build();
            // Stream the response back to the sender
            responseObserver.onNext(singleRequestResponse);
        });
        responseObserver.onCompleted();
        getContext().getSystem().stop(getSelf());
    }
}
