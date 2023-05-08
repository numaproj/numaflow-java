package io.numaproj.numaflow.function;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
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
import java.util.concurrent.ExecutionException;

import static io.numaproj.numaflow.function.FunctionConstants.EOF;
import static org.junit.Assert.fail;

public class ReduceSupervisorActorTest {

    @Test
    public void invokeSingleActor() throws RuntimeException {
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

        ReduceOutputStreamObserver outputStreamObserver = new ReduceOutputStreamObserver();

        ActorRef supervisor = actorSystem
                .actorOf(ReduceSupervisorActor
                        .props(new TestReducerFactory(), md, shutdownActor, outputStreamObserver));

        for (int i = 1; i <= 10; i++) {
            Udfunction.DatumRequest inputDatum = inDatumBuilder
                    .addKeys("reduce-test")
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
                                new TestReducerFactory(),
                                md,
                                shutdownActor,
                                new ReduceOutputStreamObserver()));

        for (int i = 1; i <= 10; i++) {
            Udfunction.DatumRequest inputDatum = Udfunction.DatumRequest.newBuilder()
                    .addKeys("reduce-test" + i)
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

    public static class TestReducerFactory extends ReducerFactory<TestReducerFactory.TestReduceHandler> {

        @Override
        public TestReduceHandler createReducer() {
            return new TestReduceHandler();
        }

        public static class TestReduceHandler extends ReduceHandler {

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
