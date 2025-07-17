package io.numaproj.numaflow.reducer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AllDeadLetters;
import akka.actor.DeadLetter;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class ShutDownActorTest {

    @Test
    public void testFailure() {
        final ActorSystem actorSystem = ActorSystem.create("test-system-1");
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        String reduceKey = "reduce-key";
        ReduceOuterClass.ReduceRequest.Payload.Builder payloadBuilder = ReduceOuterClass.ReduceRequest.Payload
                .newBuilder()
                .addKeys(reduceKey);

        ActorRef shutdownActor = actorSystem
                .actorOf(ReduceShutdownActor
                        .props(completableFuture));

        ActorRef supervisorActor = actorSystem
                .actorOf(ReduceSupervisorActor
                        .props(
                                new TestExceptionFactory(),
                                shutdownActor,
                                new ReduceOutputStreamObserver()));

        ActorRequest reduceRequest = new ActorRequest(ReduceOuterClass.ReduceRequest.newBuilder()
                .setPayload(payloadBuilder
                        .addKeys("reduce-test")
                        .setValue(ByteString.copyFromUtf8(String.valueOf(1)))
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

        try {
            completableFuture.get();
            fail("Expected the future to complete with exception");
        } catch (Exception e) {
            assertEquals("java.lang.RuntimeException: UDF Failure", e.getMessage());
        }
    }

    @Test
    public void testDeadLetterHandling() {
        final ActorSystem actorSystem = ActorSystem.create("test-system-2");
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        ActorRef shutdownActor = actorSystem
                .actorOf(ReduceShutdownActor
                        .props(completableFuture));

        actorSystem.eventStream().subscribe(shutdownActor, AllDeadLetters.class);

        ActorRef supervisorActor = actorSystem
                .actorOf(ReduceSupervisorActor
                        .props(
                                new TestExceptionFactory(),
                                shutdownActor,
                                new ReduceOutputStreamObserver()));

        DeadLetter deadLetter = new DeadLetter("dead-letter", shutdownActor, supervisorActor);
        supervisorActor.tell(deadLetter, ActorRef.noSender());

        try {
            completableFuture.get();
            fail("Expected the future to complete with exception");
        } catch (Exception e) {
            assertEquals("java.lang.Throwable: dead letters", e.getMessage());
        }
    }


    public static class TestExceptionFactory extends ReducerFactory<TestExceptionFactory.TestException> {

        @Override
        public TestException createReducer() {
            return new TestException();
        }

        public static class TestException extends Reducer {

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
