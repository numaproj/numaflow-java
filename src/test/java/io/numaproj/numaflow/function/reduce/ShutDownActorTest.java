package io.numaproj.numaflow.function.reduce;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AllDeadLetters;
import akka.actor.DeadLetter;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class ShutDownActorTest {

    @Test
    public void testFailure() {
        final ActorSystem actorSystem = ActorSystem.create("test-system-1");
        CompletableFuture<Void> completableFuture = new CompletableFuture<Void>();

        String reduceKey = "reduce-key";
        Udfunction.Datum.Builder inDatumBuilder = Udfunction.Datum.
                newBuilder().addKey(reduceKey);

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
                                new TestExceptionFactory(),
                                md,
                                shutdownActor,
                                new ReduceOutputStreamObserver()));

        Udfunction.Datum inputDatum = inDatumBuilder
                .addKey("reduce-test")
                .setValue(ByteString.copyFromUtf8(String.valueOf(1)))
                .build();
        supervisor.tell(inputDatum, ActorRef.noSender());

        try {
            completableFuture.get();
            fail("Expected the future to complete with exception");
        } catch (Exception e) {
            assertEquals(e.getMessage(), "java.lang.RuntimeException: UDF Failure");
        }
    }

    @Test
    public void testDeadLetterHandling() {
        final ActorSystem actorSystem = ActorSystem.create("test-system-2");
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        ActorRef shutdownActor = actorSystem
                .actorOf(ShutdownActor
                        .props(
                                new ReduceOutputStreamObserver(),
                                completableFuture));

        actorSystem.eventStream().subscribe(shutdownActor, AllDeadLetters.class);

        Metadata md = new MetadataImpl(
                new IntervalWindowImpl(Instant.now(), Instant.now()));

        ActorRef supervisor = actorSystem
                .actorOf(ReduceSupervisorActor
                        .props(
                                new TestExceptionFactory(),
                                md,
                                shutdownActor,
                                new ReduceOutputStreamObserver()));

        DeadLetter deadLetter = new DeadLetter("dead-letter", shutdownActor, supervisor);
        supervisor.tell(deadLetter, ActorRef.noSender());

        try {
            completableFuture.get();
            fail("Expected the future to complete with exception");
        } catch (Exception e) {
            assertEquals(e.getMessage(), "java.lang.Throwable: dead letters");
        }
    }


    public static class TestExceptionFactory extends ReducerFactory<TestExceptionFactory.TestException> {

        @Override
        public TestException createReducer() {
            return new TestException();
        }

        public static class TestException extends ReduceHandler {

            int count = 0;

            @Override
            public void addMessage(String[] key, Datum datum, Metadata md) {
                count += 1;
                throw new RuntimeException("UDF Failure");
            }

            @Override
            public Message[] getOutput(String[] key, Metadata md) {
                return new Message[]{Message.toAll(String.valueOf(count).getBytes())};
            }
        }
    }
}
