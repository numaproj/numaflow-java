package io.numaproj.numaflow.function.reduce;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import com.google.protobuf.ByteString;
import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.Function;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.ReduceOutputStreamObserver;
import io.numaproj.numaflow.function.metadata.IntervalWindowImpl;
import io.numaproj.numaflow.function.metadata.Metadata;
import io.numaproj.numaflow.function.metadata.MetadataImpl;
import io.numaproj.numaflow.function.v1.Udfunction;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;

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

        ActorRef supervisor = actorSystem
                .actorOf(ReduceSupervisorActor
                        .props(TestGroupBy.class, md, shutdownActor));

        for (int i = 1; i <= 10; i++) {
            Udfunction.Datum inputDatum = inDatumBuilder
                    .setKey("reduce-test")
                    .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                    .build();
            supervisor.tell(inputDatum, ActorRef.noSender());
        }

        Future<Object> resultFuture = Patterns
                .ask(supervisor, Function.EOF, Integer.MAX_VALUE);

        List<Future<Object>> udfResults = null;
        try {
            udfResults = (List<Future<Object>>)
                    Await.result(resultFuture, Duration.Inf());
        } catch (TimeoutException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertEquals(udfResults.size(), 1);
        udfResults.forEach(f -> {
            try {
                Message[] output = (Message[]) Await.result(f, Duration.Inf());
                Arrays.stream(output).forEach(message -> {
                    assertEquals(message.getKey(), Message.ALL);
                    assertEquals(new String(message.getValue()), "10");
                });
            } catch (TimeoutException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
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
                        .props(TestGroupBy.class, md, shutdownActor));

        for (int i = 1; i <= 10; i++) {
            Udfunction.Datum inputDatum = inDatumBuilder
                    .setKey("reduce-test" + i)
                    .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                    .build();
            supervisor.tell(inputDatum, ActorRef.noSender());
        }

        Future<Object> resultFuture = Patterns
                .ask(supervisor, Function.EOF, Integer.MAX_VALUE);

        List<Future<Object>> udfResults = null;
        try {
            udfResults = (List<Future<Object>>)
                    Await.result(resultFuture, Duration.Inf());
        } catch (TimeoutException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertEquals(udfResults.size(), 10);
        udfResults.forEach(f -> {
            try {
                Message[] output = (Message[]) Await.result(f, Duration.Inf());
                Arrays.stream(output).forEach(message -> {
                    assertEquals(message.getKey(), Message.ALL);
                    assertEquals(new String(message.getValue()), "1");
                });
            } catch (TimeoutException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static class TestGroupBy extends GroupBy {

        int count = 0;

        public TestGroupBy(String key, Metadata metadata) {
            super(key, metadata);
        }

        @Override
        public void addMessage(Datum datum) {
            count += 1;
        }

        @Override
        public Message[] getOutput() {
            return new Message[]{Message.toAll(String.valueOf(count).getBytes())};
        }
    }
}
