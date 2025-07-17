package io.numaproj.numaflow.reducer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SupervisorActorTest {

    @Test
    public void given_inputRequestsShareSameKeys_when_supervisorActorBroadcasts_then_onlyOneReducerActorGetsCreatedAndAggregatesAllRequests() throws RuntimeException {
        final ActorSystem actorSystem = ActorSystem.create("test-system-1");
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        ActorRef shutdownActor = actorSystem
                .actorOf(ReduceShutdownActor
                        .props(completableFuture));

        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();

        ActorRef supervisorActor = actorSystem
                .actorOf(ReduceSupervisorActor
                        .props(new TestReducerFactory(), shutdownActor, outputStreamObserver));

        for (int i = 1; i <= 10; i++) {
            ActorRequest reduceRequest = new ActorRequest(ReduceOuterClass.ReduceRequest
                    .newBuilder()
                    .setPayload(ReduceOuterClass.ReduceRequest.Payload
                            .newBuilder()
                            // all reduce requests share same set of keys.
                            .addAllKeys(Arrays.asList("key-1", "key-2"))
                            .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                            .build())
                    .setOperation(ReduceOuterClass.ReduceRequest.WindowOperation
                            .newBuilder()
                            .addWindows(
                                    ReduceOuterClass.Window
                                            .newBuilder()
                                            .setStart(Timestamp.newBuilder().setSeconds(60000).build())
                                            .setEnd(Timestamp.newBuilder().setSeconds(60000).build())
                                            .build()
                            ))
                    .build());
            supervisorActor.tell(reduceRequest, ActorRef.noSender());
        }
        supervisorActor.tell(Constants.EOF, ActorRef.noSender());

        try {
            completableFuture.get();
            List<ReduceOuterClass.ReduceResponse> result = outputStreamObserver.resultDatum.get();
            // the observer should receive 2 messages, one is the aggregated result, the other is the EOF response.
            assertEquals(2, result.size());
            assertEquals(
                    "10", result
                            .get(0)
                            .getResult()
                            .getValue()
                            .toStringUtf8());
            assertTrue(result
                    .get(1)
                    .getEOF());
        } catch (InterruptedException | ExecutionException e) {
            fail("Expected the future to complete without exception");
        }
    }

    @Test
    public void given_inputRequestsHaveDifferentKeySets_when_supervisorActorBroadcasts_then_multipleReducerActorsHandleKeySetsSeparately() throws RuntimeException {
        final ActorSystem actorSystem = ActorSystem.create("test-system-2");
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        int keyCount = 10;

        ActorRef shutdownActor = actorSystem
                .actorOf(ReduceShutdownActor
                        .props(completableFuture));

        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();
        ActorRef supervisorActor = actorSystem
                .actorOf(ReduceSupervisorActor
                        .props(
                                new TestReducerFactory(),
                                shutdownActor,
                                outputStreamObserver)
                );

        for (int i = 1; i <= keyCount; i++) {
            ActorRequest reduceRequest = new ActorRequest(ReduceOuterClass.ReduceRequest
                    .newBuilder()
                    .setPayload(ReduceOuterClass.ReduceRequest.Payload
                            .newBuilder()
                            // each request contain a unique set of keys.
                            .addAllKeys(Arrays.asList("shared-key", "unique-key-" + i))
                            .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                            .build())
                    .setOperation(ReduceOuterClass.ReduceRequest.WindowOperation
                            .newBuilder()
                            .addWindows(
                                    ReduceOuterClass.Window
                                            .newBuilder()
                                            .setStart(Timestamp.newBuilder().setSeconds(60000).build())
                                            .setEnd(Timestamp.newBuilder().setSeconds(60000).build())
                                            .build()
                            ))
                    .build());
            supervisorActor.tell(reduceRequest, ActorRef.noSender());
        }

        supervisorActor.tell(Constants.EOF, ActorRef.noSender());
        try {
            completableFuture.get();
            List<ReduceOuterClass.ReduceResponse> result = outputStreamObserver.resultDatum.get();
            // expect keyCount number of responses with data, plus one final EOF response.
            assertEquals(keyCount + 1, result.size());
            for (int i = 0; i < keyCount; i++) {
                ReduceOuterClass.ReduceResponse response = result.get(i);
                assertEquals("1", response.getResult().getValue().toStringUtf8());
            }
            // verify the last one is the EOF.
            assertTrue(result.get(keyCount).getEOF());
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
