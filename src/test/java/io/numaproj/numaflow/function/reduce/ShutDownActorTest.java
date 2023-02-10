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
import scala.concurrent.Future;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class ShutDownActorTest {

    public static class TestException extends GroupBy {

        int count = 0;

        public TestException(String key, Metadata metadata) {
            super(key, metadata);
        }

        @Override
        public void addMessage(Datum datum) {
            count += 1;
            throw new RuntimeException("UDF Failure");
        }

        @Override
        public Message[] getOutput() {
            return new Message[]{Message.toAll(String.valueOf(count).getBytes())};
        }
    }

    @Test
    public void testFailure() {
        final ActorSystem actorSystem = ActorSystem.create("test-system-2");
        CompletableFuture<Void> completableFuture = new CompletableFuture<Void>();

        String reduceKey = "reduce-key";
        Udfunction.Datum.Builder inDatumBuilder = Udfunction.Datum.
                newBuilder().setKey(reduceKey);

        ActorRef shutdownActor = actorSystem
                .actorOf(ShutdownActor
                        .props(new ReduceOutputStreamObserver(),
                                completableFuture));

        Metadata md = new MetadataImpl(
                new IntervalWindowImpl(Instant.now(), Instant.now()));

        ActorRef supervisor = actorSystem
                .actorOf(ReduceSupervisorActor
                        .props(TestException.class, md, shutdownActor));

        for (int i = 1; i <= 1; i++) {
            Udfunction.Datum inputDatum = inDatumBuilder
                    .setKey("reduce-test" + i)
                    .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                    .build();
            supervisor.tell(inputDatum, ActorRef.noSender());
        }

        Future<Object> resultFuture = Patterns
                .ask(supervisor, Function.EOF, Integer.MAX_VALUE);

        try {
            completableFuture.get();
            fail("Expected the future to complete with exception");
        } catch (Exception e) {
            assertEquals(e.getMessage(), "java.lang.RuntimeException: UDF Failure");
        }
    }
}
