package io.numaproj.numaflow.sessionreducer;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;
import io.numaproj.numaflow.sessionreducer.model.Datum;
import io.numaproj.numaflow.sessionreducer.model.Message;
import io.numaproj.numaflow.sessionreducer.model.OutputStreamObserver;
import io.numaproj.numaflow.sessionreducer.model.SessionReducer;
import io.numaproj.numaflow.sessionreducer.model.SessionReducerFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SupervisorActorTest {
    @Test
    // TODO - update test name - fall ALL
    public void open_expand_close() throws RuntimeException {
        final ActorSystem actorSystem = ActorSystem.create("test-system-1");
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        ActorRef shutdownActor = actorSystem
                .actorOf(ShutdownActor
                        .props(completableFuture));

        ReduceOutputStreamObserver reduceOutputStreamObserver = new ReduceOutputStreamObserver();

        ActorRef outputActor = actorSystem.actorOf(OutputActor
                .props(reduceOutputStreamObserver));

        ActorRef supervisorActor = actorSystem
                .actorOf(SupervisorActor
                        .props(
                                new TestSessionReducerFactory(),
                                shutdownActor,
                                outputActor));

        // send an OPEN request for key-1
        Sessionreduce.SessionReduceRequest openRequest = Sessionreduce.SessionReduceRequest
                .newBuilder()
                .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                        .newBuilder()
                        .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.OPEN_VALUE)
                        .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                .addAllKeys(List.of("key-1"))
                                .setStart(Timestamp
                                        .newBuilder().setSeconds(60000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(70000).build())
                                .setSlot("slot-0").build()))
                        .build())
                .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                        .addAllKeys(List.of("key-1"))
                        .setValue(ByteString.copyFromUtf8(String.valueOf(10)))
                        .build())
                .build();
        supervisorActor.tell(openRequest, ActorRef.noSender());

        // send an expand request for key-1
        Sessionreduce.SessionReduceRequest expandRequest = Sessionreduce.SessionReduceRequest
                .newBuilder()
                .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                        .newBuilder()
                        .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.EXPAND_VALUE)
                        .addAllKeyedWindows(List.of(
                                Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("key-1"))
                                        .setSlot("slot-0")
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(60000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(70000).build())
                                        .build(),
                                Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("key-1"))
                                        .setSlot("slot-0")
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(60000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(75000).build())
                                        .build()))
                        .build())
                .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                        .addAllKeys(List.of("key-1"))
                        .setValue(ByteString.copyFromUtf8(String.valueOf(10)))
                        .build())
                .build();
        supervisorActor.tell(expandRequest, ActorRef.noSender());

        // send a close request for key-1
        Sessionreduce.SessionReduceRequest closeRequest = Sessionreduce.SessionReduceRequest
                .newBuilder()
                .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                        .newBuilder()
                        .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.CLOSE_VALUE)
                        .addAllKeyedWindows(List.of(
                                Sessionreduce.KeyedWindow.newBuilder()
                                        .addAllKeys(List.of("key-1"))
                                        .setSlot("slot-0")
                                        .setStart(Timestamp
                                                .newBuilder().setSeconds(60000).build())
                                        .setEnd(Timestamp.newBuilder().setSeconds(75000).build())
                                        .build()))
                        .build())
                .build();
        supervisorActor.tell(closeRequest, ActorRef.noSender());

        // close the stream - TODO can I not close but still verify?
        /*
        supervisorActor.tell(
                Constants.EOF,
                ActorRef.noSender());
                */
        try {
            completableFuture.get();
            System.out.println(reduceOutputStreamObserver.resultDatum.toString());
            // the observer should receive 2 messages, one is the aggregated result, the other is the EOF response.
            assertEquals(2, reduceOutputStreamObserver.resultDatum.get().size());
            assertEquals("2", reduceOutputStreamObserver.resultDatum
                    .get()
                    .get(0)
                    .getResult()
                    .getValue()
                    .toStringUtf8());
            assertTrue(reduceOutputStreamObserver.resultDatum
                    .get()
                    .get(1)
                    .getEOF());
        } catch (InterruptedException | ExecutionException e) {
            fail("Expected the future to complete without exception");
        }
    }


    @Test
    // TODO - update test name - fall ALL
    public void given_inputRequestsShareSameKeys_when_supervisorActorBroadcasts_then_onlyOneReducerActorGetsCreatedAndAggregatesAllRequests() throws RuntimeException {
        final ActorSystem actorSystem = ActorSystem.create("test-system-1");
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        ActorRef shutdownActor = actorSystem
                .actorOf(ShutdownActor
                        .props(completableFuture));

        ReduceOutputStreamObserver reduceOutputStreamObserver = new ReduceOutputStreamObserver();

        ActorRef outputActor = actorSystem.actorOf(OutputActor
                .props(reduceOutputStreamObserver));

        ActorRef supervisorActor = actorSystem
                .actorOf(SupervisorActor
                        .props(
                                new TestSessionReducerFactory(),
                                shutdownActor,
                                outputActor));

        List<String> testKeys = List.of("key-1", "key-2");
        // send an OPEN request followed by 9 APPEND
        Sessionreduce.SessionReduceRequest openRequest = Sessionreduce.SessionReduceRequest
                .newBuilder()
                .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                        .newBuilder()
                        .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.OPEN_VALUE)
                        .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                .addAllKeys(testKeys)
                                .setStart(Timestamp
                                        .newBuilder().setSeconds(6000).build())
                                .setEnd(Timestamp.newBuilder().setSeconds(7000).build())
                                .setSlot("test-slot").build()))
                        .build())
                .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                        .addAllKeys(testKeys)
                        .setValue(ByteString.copyFromUtf8(String.valueOf(1)))
                        .build())
                .build();
        supervisorActor.tell(openRequest, ActorRef.noSender());

        for (int i = 2; i <= 10; i++) {
            Sessionreduce.SessionReduceRequest appendRequest = Sessionreduce.SessionReduceRequest
                    .newBuilder()
                    .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                            .newBuilder()
                            .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.APPEND_VALUE)
                            .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                    .addAllKeys(testKeys)
                                    .setStart(Timestamp
                                            .newBuilder().setSeconds(6000).build())
                                    .setEnd(Timestamp.newBuilder().setSeconds(7000).build())
                                    .setSlot("test-slot").build()))
                            .build())
                    .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                            .addAllKeys(testKeys)
                            .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
                            .build())
                    .build();
            supervisorActor.tell(appendRequest, ActorRef.noSender());
        }
        supervisorActor.tell(
                Constants.EOF,
                ActorRef.noSender());

        try {
            completableFuture.get();
            // the observer should receive 2 messages, one is the aggregated result, the other is the EOF response.
            assertEquals(2, reduceOutputStreamObserver.resultDatum.get().size());
            assertEquals("10", reduceOutputStreamObserver.resultDatum
                    .get()
                    .get(0)
                    .getResult()
                    .getValue()
                    .toStringUtf8());
            assertTrue(reduceOutputStreamObserver.resultDatum
                    .get()
                    .get(1)
                    .getEOF());
        } catch (InterruptedException | ExecutionException e) {
            fail("Expected the future to complete without exception");
        }
    }

    @Test
    public void given_inputRequestsHaveDifferentKeySets_when_supervisorActorBroadcasts_then_multipleReducerActorsHandleKeySetsSeparately() throws RuntimeException {
        final ActorSystem actorSystem = ActorSystem.create("test-system-2");
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        ActorRef shutdownActor = actorSystem
                .actorOf(ShutdownActor
                        .props(completableFuture));

        ReduceOutputStreamObserver reduceOutputStreamObserver = new ReduceOutputStreamObserver();
        ActorRef outputActor = actorSystem.actorOf(OutputActor
                .props(reduceOutputStreamObserver));
        ActorRef supervisorActor = actorSystem
                .actorOf(SupervisorActor
                        .props(
                                new TestSessionReducerFactory(),
                                shutdownActor,
                                outputActor)
                );
        for (int i = 1; i <= 10; i++) {
            Sessionreduce.SessionReduceRequest appendRequest = Sessionreduce.SessionReduceRequest
                    .newBuilder()
                    .setOperation(Sessionreduce.SessionReduceRequest.WindowOperation
                            .newBuilder()
                            .setEventValue(Sessionreduce.SessionReduceRequest.WindowOperation.Event.APPEND_VALUE)
                            .addAllKeyedWindows(List.of(Sessionreduce.KeyedWindow.newBuilder()
                                    .addAllKeys(Arrays.asList("shared-key", "unique-key-" + i))
                                    .setStart(Timestamp
                                            .newBuilder().setSeconds(6000).build())
                                    .setEnd(Timestamp.newBuilder().setSeconds(7000).build())
                                    .setSlot("test-slot").build()))
                            .build())
                    .setPayload(Sessionreduce.SessionReduceRequest.Payload.newBuilder()
                            .addAllKeys(Arrays.asList("shared-key", "unique-key-" + i))
                            .setValue(ByteString.copyFromUtf8(String.valueOf(1)))
                            .build())
                    .build();
            supervisorActor.tell(appendRequest, ActorRef.noSender());
        }

        supervisorActor.tell(
                Constants.EOF,
                ActorRef.noSender());
        try {
            completableFuture.get();
            // each reduce request generates two reduce responses, one containing the data and the other one indicating EOF.
            assertEquals(20, reduceOutputStreamObserver.resultDatum.get().size());
            for (int i = 0; i < 20; i++) {
                Sessionreduce.SessionReduceResponse response = reduceOutputStreamObserver.resultDatum
                        .get()
                        .get(i);
                assertTrue(response.getResult().getValue().toStringUtf8().equals("1")
                        || response.getEOF());
            }
        } catch (InterruptedException | ExecutionException e) {
            fail("Expected the future to complete without exception");
        }
    }

    public static class TestSessionReducerFactory extends SessionReducerFactory<TestSessionReducerFactory.TestSessionReducerHandler> {
        @Override
        public TestSessionReducerHandler createSessionReducer() {
            return new TestSessionReducerHandler();
        }

        public static class TestSessionReducerHandler extends SessionReducer {
            int count = 0;

            @Override
            public void processMessage(
                    String[] keys,
                    Datum datum,
                    OutputStreamObserver outputStreamObserver) {
                count += 1;
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
