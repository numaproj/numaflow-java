package io.numaproj.numaflow.batchmapper;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestActorRef;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.batchmap.v1.Batchmap;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.mock;

public class BatchMapActorTest {

    @Test
    public void test_process_valid_map_request() {
        BatchMapper batchMapper = mock(BatchMapper.class);
        ActorRef shutdownActor = mock(ActorRef.class);
        StreamObserver<Batchmap.BatchMapResponse> responseObserver = mock(StreamObserver.class);

        // Create the actor system
        ActorSystem system = ActorSystem.create("test-system");

        // Create the BatchMapActor
        Props props = BatchMapActor.props(batchMapper, shutdownActor, responseObserver);
        TestActorRef<BatchMapActor> actorRef = TestActorRef.create(system, props);

        // Create a DatumIteratorImpl and populate it with data
        DatumIteratorImpl datumIterator = new DatumIteratorImpl();
        List<BatchResponse> batchResponses = new ArrayList<>();
        try {
            for (int i = 0; i < 10; i++) {
                String uuid = UUID.randomUUID().toString();
                Batchmap.BatchMapRequest mapRequest = Batchmap.BatchMapRequest.newBuilder()
                        .setValue(ByteString.copyFromUtf8("test" + i))
                        .setWatermark(com.google.protobuf.Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                        .setEventTime(com.google.protobuf.Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                        .putHeaders("headerKey", "headerValue")
                        .addKeys("key1")
                        .setId(uuid)
                        .build();

                datumIterator.writeMessage(new HandlerDatum(
                        new String[]{"key1"},
                        "test".getBytes(),
                        Instant.now(),
                        Instant.now(),
                        uuid,
                        mapRequest.getHeadersMap()
                ));

                Message message = new Message("response".getBytes(), new String[]{"key1"});
                BatchResponse batchResponse = new BatchResponse(uuid).append(message);
                batchResponses.add(batchResponse);
            }
            datumIterator.writeMessage(HandlerDatum.EOF_DATUM);
            BatchResponses batchResponsesObj = new BatchResponses();
            batchResponses.forEach(batchResponsesObj::append);

            Mockito.when(batchMapper.processMessage(Mockito.any())).thenReturn(batchResponsesObj);

            // Send the datumIterator to the actor
            actorRef.tell(datumIterator, ActorRef.noSender());

            // Verify that the responseObserver received the expected responses
            Mockito.verify(responseObserver, Mockito.times(10)).onNext(Mockito.any(Batchmap.BatchMapResponse.class));
            Mockito.verify(responseObserver).onCompleted();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
