package io.numaproj.numaflow.function.reduce;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.google.protobuf.ByteString;
import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.ReduceOutputStreamObserver;
import io.numaproj.numaflow.function.metadata.IntervalWindowImpl;
import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.metadata.MetadataImpl;
import io.numaproj.numaflow.function.v1.Udfunction;
import org.junit.Test;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.numaproj.numaflow.function.Function.EOF;
import static org.junit.Assert.fail;

public class ReduceSupervisorActorTest {

    @Test
    public void invokeSingleActor() throws RuntimeException {
        final ActorSystem actorSystem = ActorSystem.create("test-system-1");
        CompletableFuture<Void> completableFuture = new CompletableFuture<Void>();

        String reduceKey = "reduce-key";
        Udfunction.Datum.Builder inDatumBuilder = Udfunction.Datum.
                newBuilder().setKey(reduceKey);

        ActorRef shutdownActor = actorSystem
                .actorOf(ShutdownActor
                        .props(
                                new ReduceOutputStreamObserver(),
                                completableFuture));

        Metadata md = new MetadataImpl(
                new IntervalWindowImpl(Instant.now(), Instant.now()));

        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();

        ActorRef supervisor = actorSystem
                .actorOf(ReduceSupervisorActor
                        .props(new TestReducerFactory(), md, shutdownActor, outputStreamObserver));

        for (int i = 1; i <= 10; i++) {
            Udfunction.Datum inputDatum = inDatumBuilder
                    .setKey("reduce-test")
                    .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                    .build();
            supervisor.tell(inputDatum, ActorRef.noSender());
        }

        supervisor.tell(EOF, ActorRef.noSender());

        try {
            completableFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            fail("Expected the future to complete without exception");
        }
    }

    @Test
    public void invokeMultipleActors() throws RuntimeException {
        final ActorSystem actorSystem = ActorSystem.create("test-system-2");
        CompletableFuture<Void> completableFuture = new CompletableFuture<Void>();

        String reduceKey = "reduce-key";
        Udfunction.Datum.Builder inDatumBuilder = Udfunction.Datum.
                newBuilder().setKey(reduceKey);

        ActorRef shutdownActor = actorSystem
                .actorOf(ShutdownActor
                        .props(
                                new ReduceOutputStreamObserver(),
                                completableFuture));

        Metadata md = new MetadataImpl(
                new IntervalWindowImpl(Instant.now(), Instant.now()));

        ActorRef supervisor = actorSystem
                .actorOf(ReduceSupervisorActor
                        .props(
                                new TestReducerFactory(),
                                md,
                                shutdownActor,
                                new ReduceOutputStreamObserver()));

        for (int i = 1; i <= 10; i++) {
            Udfunction.Datum inputDatum = inDatumBuilder
                    .setKey("reduce-test" + i)
                    .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                    .build();
            supervisor.tell(inputDatum, ActorRef.noSender());
        }

        supervisor.tell(EOF, ActorRef.noSender());
        try {
            completableFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            fail("Expected the future to complete without exception");
        }
    }

    public static class TestReducerFactory extends ReducerFactory<TestReducerFactory.TestReducer> {

        @Override
        public TestReducer createReducer() {
            return new TestReducer();
        }

        public static class TestReducer extends Reducer {

            int count = 0;

            @Override
            public void addMessage(String key, Datum datum, Metadata md) {
                count += 1;
            }

            @Override
            public Message[] getOutput(String key, Metadata md) {
                return new Message[]{Message.toAll(String.valueOf(count).getBytes())};
            }
        }
    }
}
