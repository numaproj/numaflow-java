package io.numaproj.numaflow.reducestreamer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.AllDeadLetters;
import akka.actor.DeadLetter;
import com.google.protobuf.ByteString;
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
                .actorOf(io.numaproj.numaflow.reducestreamer.ReduceShutdownActor
                        .props(completableFuture));

        Metadata md = new MetadataImpl(
                new IntervalWindowImpl(Instant.now(), Instant.now()));

        io.numaproj.numaflow.reducestreamer.ReduceOutputStreamObserver reduceOutputStreamObserver = new io.numaproj.numaflow.reducestreamer.ReduceOutputStreamObserver();

        ActorRef responseStreamActor = actorSystem.actorOf(io.numaproj.numaflow.reducestreamer.ResponseStreamActor
                .props(reduceOutputStreamObserver, md));

        ActorRef supervisor = actorSystem
                .actorOf(io.numaproj.numaflow.reducestreamer.ReduceSupervisorActor
                        .props(
                                new TestExceptionFactory(),
                                md,
                                shutdownActor,
                                responseStreamActor,
                                new io.numaproj.numaflow.reducestreamer.ReduceOutputStreamObserver()));

        io.numaproj.numaflow.reducestreamer.ActorRequest reduceRequest = new io.numaproj.numaflow.reducestreamer.ActorRequest(
                ReduceOuterClass.ReduceRequest.newBuilder()
                        .setPayload(payloadBuilder
                                .addKeys("reduce-test")
                                .setValue(ByteString.copyFromUtf8(String.valueOf(1)))
                                .build())
                        .build());
        supervisor.tell(reduceRequest, ActorRef.noSender());

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
                .actorOf(io.numaproj.numaflow.reducestreamer.ReduceShutdownActor
                        .props(completableFuture));

        actorSystem.eventStream().subscribe(shutdownActor, AllDeadLetters.class);

        Metadata md = new MetadataImpl(
                new IntervalWindowImpl(Instant.now(), Instant.now()));

        io.numaproj.numaflow.reducestreamer.ReduceOutputStreamObserver reduceOutputStreamObserver = new io.numaproj.numaflow.reducestreamer.ReduceOutputStreamObserver();

        ActorRef responseStreamActor = actorSystem.actorOf(io.numaproj.numaflow.reducestreamer.ResponseStreamActor
                .props(reduceOutputStreamObserver, md));

        ActorRef supervisor = actorSystem
                .actorOf(io.numaproj.numaflow.reducestreamer.ReduceSupervisorActor
                        .props(
                                new TestExceptionFactory(),
                                md,
                                shutdownActor,
                                responseStreamActor,
                                reduceOutputStreamObserver));

        DeadLetter deadLetter = new DeadLetter("dead-letter", shutdownActor, supervisor);
        supervisor.tell(deadLetter, ActorRef.noSender());

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
