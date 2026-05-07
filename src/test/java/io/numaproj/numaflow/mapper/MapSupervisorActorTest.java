package io.numaproj.numaflow.mapper;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.map.v1.MapOuterClass;
import io.numaproj.numaflow.shared.InputStreamError;
import org.junit.Test;

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

public class MapSupervisorActorTest {

    @Test
    public void given_inputStreamError_when_supervisorHandlesIt_then_shutdownCompletesWithoutResponseWrite() throws Exception {
        ActorSystem actorSystem = ActorSystem.create("mapper-input-error-test");
        CompletableFuture<Void> shutdownSignal = new CompletableFuture<>();
        RecordingObserver responseObserver = new RecordingObserver();
        RuntimeException streamError = new RuntimeException("client cancelled");

        try {
            ActorRef supervisor = actorSystem.actorOf(
                    MapSupervisorActor.props(new BlockingMapper(new CountDownLatch(0), new CountDownLatch(0)),
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
        ActorSystem actorSystem = ActorSystem.create("mapper-drain-test");
        CompletableFuture<Void> shutdownSignal = new CompletableFuture<>();
        CountDownLatch mapperStarted = new CountDownLatch(1);
        CountDownLatch releaseMapper = new CountDownLatch(1);
        RecordingObserver responseObserver = new RecordingObserver();

        try {
            ActorRef supervisor = actorSystem.actorOf(
                    MapSupervisorActor.props(new BlockingMapper(mapperStarted, releaseMapper),
                            responseObserver,
                            shutdownSignal));

            supervisor.tell(mapRequest(), ActorRef.noSender());
            assertTrue(mapperStarted.await(2, TimeUnit.SECONDS));
            supervisor.tell(Constants.EOF, ActorRef.noSender());

            Thread.sleep(100);
            assertFalse(responseObserver.completed.isDone());

            releaseMapper.countDown();

            assertTrue(responseObserver.completed.get(2, TimeUnit.SECONDS));
            assertEquals(List.of("next", "completed"), responseObserver.events);
        } finally {
            actorSystem.terminate();
        }
    }

    @Test
    public void given_childFailure_when_childDrains_then_shutdownCompletesExceptionally() throws Exception {
        ActorSystem actorSystem = ActorSystem.create("mapper-failure-test");
        CompletableFuture<Void> shutdownSignal = new CompletableFuture<>();
        RecordingObserver responseObserver = new RecordingObserver();

        try {
            ActorRef supervisor = actorSystem.actorOf(
                    MapSupervisorActor.props(new FailingMapper(), responseObserver, shutdownSignal));

            supervisor.tell(mapRequest(), ActorRef.noSender());

            assertCompletesExceptionally(shutdownSignal);
            assertTrue(responseObserver.error.get(2, TimeUnit.SECONDS) instanceof RuntimeException);
        } finally {
            actorSystem.terminate();
        }
    }

    @Test
    public void given_responseObserverThrowsOnNext_when_supervisorHandlesResponse_then_shutdownCompletesExceptionally()
            throws Exception {
        ActorSystem actorSystem = ActorSystem.create("mapper-closed-response-test");
        CompletableFuture<Void> shutdownSignal = new CompletableFuture<>();

        try {
            ActorRef supervisor = actorSystem.actorOf(
                    MapSupervisorActor.props(new BlockingMapper(new CountDownLatch(0), new CountDownLatch(0)),
                            new FailingOnNextObserver(),
                            shutdownSignal));

            supervisor.tell(mapRequest(), ActorRef.noSender());

            ExecutionException exception = assertCompletesExceptionally(shutdownSignal);
            assertTrue(exception.getCause() instanceof IllegalStateException);
            assertEquals("call already closed", exception.getCause().getMessage());
        } finally {
            actorSystem.terminate();
        }
    }

    private static MapOuterClass.MapRequest mapRequest() {
        return MapOuterClass.MapRequest.newBuilder()
                .setId("id")
                .setRequest(MapOuterClass.MapRequest.Request.newBuilder()
                        .setValue(ByteString.copyFromUtf8("input"))
                        .addKeys("key")
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

    private static class BlockingMapper extends Mapper {
        private final CountDownLatch started;
        private final CountDownLatch release;

        private BlockingMapper(CountDownLatch started, CountDownLatch release) {
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
                    .addMessage(new Message("output".getBytes()))
                    .build();
        }
    }

    private static class FailingMapper extends Mapper {
        @Override
        public MessageList processMessage(String[] keys, Datum datum) {
            throw new RuntimeException("user failure");
        }
    }

    private static class RecordingObserver implements StreamObserver<MapOuterClass.MapResponse> {
        private final List<String> events = new CopyOnWriteArrayList<>();
        private final CompletableFuture<Boolean> completed = new CompletableFuture<>();
        private final CompletableFuture<Throwable> error = new CompletableFuture<>();

        @Override
        public void onNext(MapOuterClass.MapResponse mapResponse) {
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

    private static class FailingOnNextObserver implements StreamObserver<MapOuterClass.MapResponse> {
        @Override
        public void onNext(MapOuterClass.MapResponse mapResponse) {
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
