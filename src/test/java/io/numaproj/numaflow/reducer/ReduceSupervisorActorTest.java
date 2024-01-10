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
import static org.junit.Assert.fail;

public class ReduceSupervisorActorTest {

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

        ActorRef supervisor = actorSystem
                .actorOf(ReduceSupervisorActor
                        .props(new TestReducerFactory(), md, shutdownActor, outputStreamObserver));

        for (int i = 1; i <= 10; i++) {
            ReduceOuterClass.ReduceRequest reduceRequest = ReduceOuterClass.ReduceRequest
                    .newBuilder()
                    .setPayload(ReduceOuterClass.ReduceRequest.Payload
                            .newBuilder()
                            // all reduce requests share same set of keys.
                            .addAllKeys(Arrays.asList("key-1", "key-2"))
                            .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                            .build())
                    .build();
            supervisor.tell(reduceRequest, ActorRef.noSender());
        }
        supervisor.tell(Constants.EOF, ActorRef.noSender());

        try {
            completableFuture.get();
            assertEquals(1, outputStreamObserver.resultDatum.get().size());
            assertEquals("10", outputStreamObserver.resultDatum
                    .get()
                    .get(0)
                    .getResult()
                    .getValue()
                    .toStringUtf8());
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
        ActorRef supervisor = actorSystem
                .actorOf(ReduceSupervisorActor
                        .props(
                                new TestReducerFactory(),
                                md,
                                shutdownActor,
                                outputStreamObserver)
                );

        for (int i = 1; i <= 10; i++) {
            ReduceOuterClass.ReduceRequest reduceRequest = ReduceOuterClass.ReduceRequest
                    .newBuilder()
                    .setPayload(ReduceOuterClass.ReduceRequest.Payload
                            .newBuilder()
                            // each request contain a unique set of keys.
                            .addAllKeys(Arrays.asList("shared-key", "unique-key-" + i))
                            .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                            .build())
                    .build();
            supervisor.tell(reduceRequest, ActorRef.noSender());
        }

        supervisor.tell(Constants.EOF, ActorRef.noSender());
        try {
            completableFuture.get();
            // the outputStreamObserver should get updated 10 times, each time with value 1.
            assertEquals(10, outputStreamObserver.resultDatum.get().size());
            for (int i = 0; i < 10; i++) {
                assertEquals("1", outputStreamObserver.resultDatum
                        .get()
                        .get(i)
                        .getResult()
                        .getValue()
                        .toStringUtf8());
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
