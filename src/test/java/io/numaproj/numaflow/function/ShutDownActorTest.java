package io.numaproj.numaflow.function;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AllDeadLetters;
import akka.actor.DeadLetter;
import com.google.protobuf.ByteString;
import io.numaproj.numaflow.function.handlers.ReduceHandler;
import io.numaproj.numaflow.function.handlers.ReducerFactory;
import io.numaproj.numaflow.function.interfaces.Datum;
import io.numaproj.numaflow.function.interfaces.Metadata;
import io.numaproj.numaflow.function.metadata.IntervalWindowImpl;
import io.numaproj.numaflow.function.metadata.MetadataImpl;
import io.numaproj.numaflow.function.types.Message;
import io.numaproj.numaflow.function.types.MessageList;
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
        Udfunction.DatumRequest.Builder inDatumBuilder = Udfunction.DatumRequest.
                newBuilder().addKeys(reduceKey);

        ActorRef shutdownActor = actorSystem
                .actorOf(ReduceShutdownActor
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

        Udfunction.DatumRequest inputDatum = inDatumBuilder
                .addKeys("reduce-test")
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
                .actorOf(ReduceShutdownActor
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
            public void addMessage(String[] keys, Datum datum, Metadata md) {
                count += 1;
                throw new RuntimeException("UDF Failure");
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
