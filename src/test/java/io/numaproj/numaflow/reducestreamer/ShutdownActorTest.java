package io.numaproj.numaflow.reducestreamer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AllDeadLetters;
import akka.actor.DeadLetter;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import io.numaproj.numaflow.reducestreamer.model.Datum;
import io.numaproj.numaflow.reducestreamer.model.Message;
import io.numaproj.numaflow.reducestreamer.model.Metadata;
import io.numaproj.numaflow.reducestreamer.model.OutputStreamObserver;
import io.numaproj.numaflow.reducestreamer.model.ReduceStreamer;
import io.numaproj.numaflow.reducestreamer.model.ReduceStreamerFactory;
import org.junit.Test;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class ShutdownActorTest {
    @Test
    public void testFailure() {
        final ActorSystem actorSystem = ActorSystem.create("test-system-1");
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        String reduceKey = "reduce-key";
        ReduceOuterClass.ReduceRequest.Payload.Builder payloadBuilder = ReduceOuterClass.ReduceRequest.Payload
                .newBuilder()
                .addKeys(reduceKey);

        ActorRef shutdownActor = actorSystem
                .actorOf(ShutdownActor
                        .props(completableFuture));

        io.numaproj.numaflow.reducestreamer.ReduceOutputStreamObserver reduceOutputStreamObserver = new io.numaproj.numaflow.reducestreamer.ReduceOutputStreamObserver();

        ActorRef outputActor = actorSystem.actorOf(OutputActor
                .props(reduceOutputStreamObserver));

        ActorRef supervisorActor = actorSystem
                .actorOf(SupervisorActor
                        .props(
                                new TestExceptionFactory(),
                                shutdownActor,
                                outputActor));

        io.numaproj.numaflow.reducestreamer.ActorRequest reduceRequest = new io.numaproj.numaflow.reducestreamer.ActorRequest(
                ReduceOuterClass.ReduceRequest.newBuilder()
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

        io.numaproj.numaflow.reducestreamer.ReduceOutputStreamObserver reduceOutputStreamObserver = new io.numaproj.numaflow.reducestreamer.ReduceOutputStreamObserver();

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


    public static class TestExceptionFactory extends ReduceStreamerFactory<TestExceptionFactory.TestException> {

        @Override
        public TestException createReduceStreamer() {
            return new TestException();
        }

        public static class TestException extends ReduceStreamer {

            int count = 0;

            @Override
            public void processMessage(
                    String[] keys,
                    Datum datum,
                    OutputStreamObserver outputStream,
                    Metadata md) {
                count += 1;
                throw new RuntimeException("UDF Failure");
            }

            @Override
            public void handleEndOfStream(
                    String[] keys,
                    OutputStreamObserver outputStreamObserver,
                    Metadata md) {
                outputStreamObserver.send(new Message(String.valueOf(count).getBytes()));
            }
        }
    }
}
