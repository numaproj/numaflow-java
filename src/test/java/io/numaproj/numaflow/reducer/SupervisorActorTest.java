package io.numaproj.numaflow.reducer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.google.protobuf.ByteString;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import io.numaproj.numaflow.reducer.metadata.IntervalWindowImpl;
import io.numaproj.numaflow.reducer.metadata.MetadataImpl;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SupervisorActorTest {

    @Test
    public void given_inputRequestsShareSameKeys_when_supervisorActorBroadcasts_then_onlyOneReducerActorGetsCreatedAndAggregatesAllRequests() throws RuntimeException {
        final ActorSystem actorSystem = ActorSystem.create("test-system-1");
        CompletableFuture<Void> completableFuture = new CompletableFuture<Void>();

        ActorRef shutdownActor = actorSystem
                .actorOf(ReduceShutdownActor
                        .props(completableFuture));

        Metadata md = new MetadataImpl(
                new IntervalWindowImpl(Instant.now(), Instant.now()));

        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();

        ActorRef supervisorActor = actorSystem
                .actorOf(ReduceSupervisorActor
                        .props(new TestReducerFactory(), md, shutdownActor, outputStreamObserver));

        for (int i = 1; i <= 10; i++) {
            ActorRequest reduceRequest = new ActorRequest(ReduceOuterClass.ReduceRequest
                    .newBuilder()
                    .setPayload(ReduceOuterClass.ReduceRequest.Payload
                            .newBuilder()
                            // all reduce requests share same set of keys.
                            .addAllKeys(Arrays.asList("key-1", "key-2"))
                            .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                            .build())
                    .build());
            supervisorActor.tell(reduceRequest, ActorRef.noSender());
        }
        supervisorActor.tell(Constants.EOF, ActorRef.noSender());

        try {
            completableFuture.get();
            // the observer should receive 2 messages, one is the aggregated result, the other is the EOF response.
            assertEquals(2, outputStreamObserver.resultDatum.get().size());
            assertEquals("10", outputStreamObserver.resultDatum
                    .get()
                    .get(0)
                    .getResult()
                    .getValue()
                    .toStringUtf8());
            assertEquals(true, outputStreamObserver.resultDatum
                    .get()
                    .get(1)
                    .getEOF());
        } catch (InterruptedException | ExecutionException e) {
            fail("Expected the future to complete without exception");
        }
    }

    @Test
    public void given_inputRequestsHaveDifferentKeySets_when_supervisorActorBroadcasts_then_multipleReducerActorsHandleKeySetsSeparately() throws RuntimeException {
        final ActorSystem actorSystem = ActorSystem.create("test-system-2");
        CompletableFuture<Void> completableFuture = new CompletableFuture<Void>();

        ActorRef shutdownActor = actorSystem
                .actorOf(ReduceShutdownActor
                        .props(completableFuture));

        Metadata md = new MetadataImpl(
                new IntervalWindowImpl(Instant.now(), Instant.now()));

        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();
        ActorRef supervisorActor = actorSystem
                .actorOf(ReduceSupervisorActor
                        .props(
                                new TestReducerFactory(),
                                md,
                                shutdownActor,
                                outputStreamObserver)
                );

        for (int i = 1; i <= 10; i++) {
            ActorRequest reduceRequest = new ActorRequest(ReduceOuterClass.ReduceRequest
                    .newBuilder()
                    .setPayload(ReduceOuterClass.ReduceRequest.Payload
                            .newBuilder()
                            // each request contain a unique set of keys.
                            .addAllKeys(Arrays.asList("shared-key", "unique-key-" + i))
                            .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                            .build())
                    .build());
            supervisorActor.tell(reduceRequest, ActorRef.noSender());
        }

        supervisorActor.tell(Constants.EOF, ActorRef.noSender());
        try {
            completableFuture.get();
            // each reduce request generates two reduce responses, one containing the data and the other one indicating EOF.
            assertEquals(20, outputStreamObserver.resultDatum.get().size());
            for (int i = 0; i < 20; i++) {
                ReduceOuterClass.ReduceResponse response = outputStreamObserver.resultDatum
                        .get()
                        .get(i);
                assertTrue(response.getResult().getValue().toStringUtf8().equals("1")
                        || response.getEOF());
            }
        } catch (InterruptedException | ExecutionException e) {
            fail("Expected the future to complete without exception");
        }
    }

    public static class TestReducerFactory extends ReducerFactory<TestReducerFactory.TestReduceHandler> {

        @Override
        public TestReduceHandler createReducer() {
            return new TestReduceHandler();
        }

        public static class TestReduceHandler extends Reducer {

            int count = 0;

            @Override
            public void addMessage(String[] keys, Datum datum, Metadata md) {
                count += 1;
            }

            @Override
            public MessageList getOutput(String[] keys, Metadata md) {
                return MessageList
                        .newBuilder()
                        .addMessage(new Message(String.valueOf(count).getBytes()))
                        .build();
            }
        }
    }
}
