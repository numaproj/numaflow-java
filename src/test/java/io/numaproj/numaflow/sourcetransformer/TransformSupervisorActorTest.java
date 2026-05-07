package io.numaproj.numaflow.sourcetransformer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.shared.InputStreamError;
import io.numaproj.numaflow.sourcetransformer.v1.Sourcetransformer;
import org.junit.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TransformSupervisorActorTest {

    @Test
    public void given_inputStreamError_when_supervisorHandlesIt_then_shutdownCompletesWithoutResponseWrite() throws Exception {
        ActorSystem actorSystem = ActorSystem.create("transform-input-error-test");
        CompletableFuture<Void> shutdownSignal = new CompletableFuture<>();
        RecordingObserver responseObserver = new RecordingObserver();
        RuntimeException streamError = new RuntimeException("client cancelled");

        try {
            ActorRef supervisor = actorSystem.actorOf(
                    TransformSupervisorActor.props(new BlockingTransformer(new CountDownLatch(0), new CountDownLatch(0)),
                            responseObserver,
                            shutdownSignal));

            supervisor.tell(new InputStreamError(streamError), ActorRef.noSender());

            ExecutionException exception = assertCompletesExceptionally(shutdownSignal);
            assertSame(streamError, exception.getCause());
            assertTrue(responseObserver.events.isEmpty());
        } finally {
            actorSystem.terminate();
        }
    }

    @Test
    public void given_eofArrivesBeforeChildResponse_when_childDrains_then_responseIsSentBeforeCompleted() throws Exception {
        ActorSystem actorSystem = ActorSystem.create("transform-drain-test");
        CompletableFuture<Void> shutdownSignal = new CompletableFuture<>();
        CountDownLatch transformerStarted = new CountDownLatch(1);
        CountDownLatch releaseTransformer = new CountDownLatch(1);
        RecordingObserver responseObserver = new RecordingObserver();

        try {
            ActorRef supervisor = actorSystem.actorOf(
                    TransformSupervisorActor.props(new BlockingTransformer(transformerStarted, releaseTransformer),
                            responseObserver,
                            shutdownSignal));

            supervisor.tell(transformRequest(), ActorRef.noSender());
            assertTrue(transformerStarted.await(2, TimeUnit.SECONDS));
            supervisor.tell(Constants.EOF, ActorRef.noSender());

            Thread.sleep(100);
            assertFalse(responseObserver.completed.isDone());

            releaseTransformer.countDown();

            assertTrue(responseObserver.completed.get(2, TimeUnit.SECONDS));
            assertEquals(List.of("next", "completed"), responseObserver.events);
        } finally {
            actorSystem.terminate();
        }
    }

    @Test
    public void given_childFailure_when_childDrains_then_shutdownCompletesExceptionally() throws Exception {
        ActorSystem actorSystem = ActorSystem.create("transform-failure-test");
        CompletableFuture<Void> shutdownSignal = new CompletableFuture<>();
        RecordingObserver responseObserver = new RecordingObserver();

        try {
            ActorRef supervisor = actorSystem.actorOf(
                    TransformSupervisorActor.props(new FailingTransformer(), responseObserver, shutdownSignal));

            supervisor.tell(transformRequest(), ActorRef.noSender());

            assertCompletesExceptionally(shutdownSignal);
            assertTrue(responseObserver.error.get(2, TimeUnit.SECONDS) instanceof RuntimeException);
        } finally {
            actorSystem.terminate();
        }
    }

    @Test
    public void given_responseObserverThrowsOnNext_when_supervisorHandlesResponse_then_shutdownCompletesExceptionally()
            throws Exception {
        ActorSystem actorSystem = ActorSystem.create("transform-closed-response-test");
        CompletableFuture<Void> shutdownSignal = new CompletableFuture<>();

        try {
            ActorRef supervisor = actorSystem.actorOf(
                    TransformSupervisorActor.props(new BlockingTransformer(new CountDownLatch(0), new CountDownLatch(0)),
                            new FailingOnNextObserver(),
                            shutdownSignal));

            supervisor.tell(transformRequest(), ActorRef.noSender());

            ExecutionException exception = assertCompletesExceptionally(shutdownSignal);
            assertTrue(exception.getCause() instanceof IllegalStateException);
            assertEquals("call already closed", exception.getCause().getMessage());
        } finally {
            actorSystem.terminate();
        }
    }

    private static Sourcetransformer.SourceTransformRequest transformRequest() {
        return Sourcetransformer.SourceTransformRequest.newBuilder()
                .setRequest(Sourcetransformer.SourceTransformRequest.Request.newBuilder()
                        .setValue(ByteString.copyFromUtf8("input"))
                        .addKeys("key")
                        .setId("id")
                        .build())
                .build();
    }

    private static ExecutionException assertCompletesExceptionally(CompletableFuture<Void> future)
            throws InterruptedException {
        try {
            future.get(2, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return e;
        } catch (java.util.concurrent.TimeoutException e) {
            throw new AssertionError("expected future to complete exceptionally", e);
        }
        throw new AssertionError("expected future to complete exceptionally");
    }

    private static class BlockingTransformer extends SourceTransformer {
        private final CountDownLatch started;
        private final CountDownLatch release;

        private BlockingTransformer(CountDownLatch started, CountDownLatch release) {
            this.started = started;
            this.release = release;
        }

        @Override
        public MessageList processMessage(String[] keys, Datum datum) {
            started.countDown();
            try {
                release.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return MessageList.newBuilder()
                    .addMessage(new Message("output".getBytes(), Instant.EPOCH))
                    .build();
        }
    }

    private static class FailingTransformer extends SourceTransformer {
        @Override
        public MessageList processMessage(String[] keys, Datum datum) {
            throw new RuntimeException("user failure");
        }
    }

    private static class RecordingObserver implements StreamObserver<Sourcetransformer.SourceTransformResponse> {
        private final List<String> events = new CopyOnWriteArrayList<>();
        private final CompletableFuture<Boolean> completed = new CompletableFuture<>();
        private final CompletableFuture<Throwable> error = new CompletableFuture<>();

        @Override
        public void onNext(Sourcetransformer.SourceTransformResponse sourceTransformResponse) {
            events.add("next");
        }

        @Override
        public void onError(Throwable throwable) {
            events.add("error");
            error.complete(throwable);
        }

        @Override
        public void onCompleted() {
            events.add("completed");
            completed.complete(true);
        }
    }

    private static class FailingOnNextObserver implements StreamObserver<Sourcetransformer.SourceTransformResponse> {
        @Override
        public void onNext(Sourcetransformer.SourceTransformResponse sourceTransformResponse) {
            throw new IllegalStateException("call already closed");
        }

        @Override
        public void onError(Throwable throwable) {
        }

        @Override
        public void onCompleted() {
        }
    }
}
