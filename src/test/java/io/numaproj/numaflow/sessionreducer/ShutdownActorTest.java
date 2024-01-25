package io.numaproj.numaflow.sessionreducer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AllDeadLetters;
import akka.actor.DeadLetter;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;
import io.numaproj.numaflow.sessionreducer.model.Datum;
import io.numaproj.numaflow.sessionreducer.model.Message;
import io.numaproj.numaflow.sessionreducer.model.OutputStreamObserver;
import io.numaproj.numaflow.sessionreducer.model.SessionReducer;
import io.numaproj.numaflow.sessionreducer.model.SessionReducerFactory;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ShutdownActorTest {
    // TODO - rename - UDF throws
    // TODO - add one more - supervisor throws
    @Test
    public void testFailure() {
        final ActorSystem actorSystem = ActorSystem.create("test-system-1");
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        List<String> keys = List.of("reduceKey");
        Sessionreduce.SessionReduceRequest.Payload.Builder payloadBuilder = Sessionreduce.SessionReduceRequest.Payload
                .newBuilder()
                .addAllKeys(keys);

        ActorRef shutdownActor = actorSystem
                .actorOf(ShutdownActor
                        .props(completableFuture));

        ReduceOutputStreamObserver reduceOutputStreamObserver = new ReduceOutputStreamObserver();

        ActorRef outputActor = actorSystem.actorOf(OutputActor
                .props(reduceOutputStreamObserver));

        ActorRef supervisorActor = actorSystem
                .actorOf(SupervisorActor
                        .props(
                                new TestExceptionFactory(),
                                shutdownActor,
                                outputActor));

        Sessionreduce.SessionReduceRequest request = Sessionreduce.SessionReduceRequest
                .newBuilder()
                .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                        .newBuilder()
                        .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.APPEND_VALUE)
                        .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                .addAllKeys(keys)
                                .setStart(Timestamp
                                        .newBuilder().setSeconds(6000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(7000).build())
                                .setSlot("test-slot").build()))
                        .build())
                .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                        .addAllKeys(keys)
                        .setValue(ByteString.copyFromUtf8(String.valueOf(1)))
                        .build())
                .build();

        supervisorActor.tell(request, ActorRef.noSender());

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
                        .props(completableFuture));

        actorSystem.eventStream().subscribe(shutdownActor, AllDeadLetters.class);

        ReduceOutputStreamObserver reduceOutputStreamObserver = new ReduceOutputStreamObserver();

        ActorRef outputActor = actorSystem.actorOf(OutputActor
                .props(reduceOutputStreamObserver));

        ActorRef supervisorActor = actorSystem
                .actorOf(SupervisorActor
                        .props(
                                new TestExceptionFactory(),
                                shutdownActor,
                                outputActor));

        DeadLetter deadLetter = new DeadLetter("dead-letter", shutdownActor, supervisorActor);
        supervisorActor.tell(deadLetter, ActorRef.noSender());

        try {
            completableFuture.get();
            fail("Expected the future to complete with exception");
        } catch (Exception e) {
            assertEquals(e.getMessage(), "java.lang.Throwable: dead letters");
        }
    }


    public static class TestExceptionFactory extends SessionReducerFactory<TestExceptionFactory.TestException> {

        @Override
        public TestException createSessionReducer() {
            return new TestException();
        }

        public static class TestException extends SessionReducer {

            int count = 0;

            @Override
            public void processMessage(
                    String[] keys,
                    Datum datum,
                    OutputStreamObserver outputStreamObserver) {
                count += 1;
                System.out.println("I am throwing.");
                throw new RuntimeException("UDF Failure");
            }

            @Override
            public void handleEndOfStream(
                    String[] keys,
                    OutputStreamObserver outputStreamObserver) {
                outputStreamObserver.send(new Message(String.valueOf(count).getBytes()));
            }

            @Override
            public byte[] accumulator() {
                return new byte[0];
            }

            @Override
            public void mergeAccumulator(byte[] accumulator) {

            }
        }
    }
}
