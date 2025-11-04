package io.numaproj.numaflow.sinker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.sink.v1.SinkGrpc;
import io.numaproj.numaflow.sink.v1.SinkOuterClass;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Slf4j
@RunWith(JUnit4.class)
public class ServerTest {
  private static final String processedIdSuffix = "-id-processed";
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private Server server;
  private ManagedChannel inProcessChannel;

  @Before
  public void setUp() throws Exception {
    String serverName = InProcessServerBuilder.generateName();

    GRPCConfig grpcServerConfig =
        GRPCConfig.newBuilder()
            .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
            .socketPath(Constants.DEFAULT_SOCKET_PATH)
            .infoFilePath("/tmp/numaflow-test-server-info)")
            .build();

    server = new Server(grpcServerConfig, new TestSinkFn(), null, serverName);

    server.start();

    inProcessChannel =
        grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build());
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  @Test
  public void testServer() {
      // Test for coverage for different constructors for Server
      Sinker sinker = new TestSinkFn();
      Server server = new Server(sinker);
      try {
          server.start();
          server.stop();
      } catch (Exception e) {
          // Interrupted exceptions are let go
          assertFalse(e instanceof RuntimeException);
      }
  }

  @Test
  public void sinkerSuccess() {
    int batchSize = 6;
    int numBatches = 8;
    // create an output stream observer
    SinkOutputStreamObserver outputStreamObserver = new SinkOutputStreamObserver();

    StreamObserver<SinkOuterClass.SinkRequest> inputStreamObserver =
        SinkGrpc.newStub(inProcessChannel).sinkFn(outputStreamObserver);

    String actualId = "sink_test_id";
    String expectedId = actualId + processedIdSuffix;

    // Send a handshake request
    SinkOuterClass.SinkRequest handshakeRequest =
        SinkOuterClass.SinkRequest.newBuilder()
            .setHandshake(SinkOuterClass.Handshake.newBuilder().setSot(true).build())
            .build();
    inputStreamObserver.onNext(handshakeRequest);

    for (int i = 1; i <= batchSize * numBatches; i++) {
      String[] keys;
      if (i % 2 == 0) {
        keys = new String[] {"valid-key"};
      } else if (i % 3 == 0) {
        keys = new String[] {"fallback-key"};
      } else if (i % 5 == 0) {
          keys = new String[] {"onsuccess-key"};
      } else if (i % 7 == 0) {
          keys = new String[] {"serve-key"};
      } else {
          keys = new String[] {"invalid-key"};
      }

      SinkOuterClass.SinkRequest.Request request =
          SinkOuterClass.SinkRequest.Request.newBuilder()
              .setValue(ByteString.copyFromUtf8(String.valueOf(i)))
              .setId(actualId)
              .addAllKeys(List.of(keys))
              .build();

      SinkOuterClass.SinkRequest sinkRequest =
          SinkOuterClass.SinkRequest.newBuilder().setRequest(request).build();
      inputStreamObserver.onNext(sinkRequest);

      // If it's the end of the batch, send an EOT message
      if (i % batchSize == 0) {
        SinkOuterClass.SinkRequest eotRequest =
            SinkOuterClass.SinkRequest.newBuilder()
                .setStatus(SinkOuterClass.TransmissionStatus.newBuilder().setEot(true).build())
                .build();
        inputStreamObserver.onNext(eotRequest);
      }
    }

    inputStreamObserver.onCompleted();

    while (!outputStreamObserver.completed.get())
      ;
    List<SinkOuterClass.SinkResponse> responseList = outputStreamObserver.getSinkResponse();
    // We expect numBatches * 2 + 1 responses,
    // with the first one being the handshake response
    // numBatches responses for the EOT messages
    // numBatches responses for all the batches.
    assertEquals(numBatches * 2 + 1, responseList.size());
    // first response is the handshake response
    assertTrue(responseList.get(0).getHandshake().getSot());
    responseList = responseList.subList(1, responseList.size());
    for (SinkOuterClass.SinkResponse response : responseList) {
      response
          .getResultsList()
          .forEach(
              result -> {
                if (result.getStatus() == SinkOuterClass.Status.FAILURE) {
                  assertEquals("error message", result.getErrMsg());
                } else if (result.getStatus() == SinkOuterClass.Status.FALLBACK) {
                    assertEquals(result.getId(), expectedId);
                    assertEquals(SinkOuterClass.Status.FALLBACK, result.getStatus());
                } else if (result.getStatus() == SinkOuterClass.Status.ON_SUCCESS) {
                    assertEquals(result.getId(), expectedId);
                    assertEquals(SinkOuterClass.Status.ON_SUCCESS, result.getStatus());
                } else if (result.getStatus() == SinkOuterClass.Status.SERVE) {
                    assertEquals(result.getId(), expectedId);
                    assertEquals(SinkOuterClass.Status.SERVE, result.getStatus());
                } else {
                    assertEquals(result.getId(), expectedId);
                    assertEquals(SinkOuterClass.Status.SUCCESS, result.getStatus());
                }
              });
    }
  }

  @Slf4j
  private static class TestSinkFn extends Sinker {
    @Override
    public ResponseList processMessages(DatumIterator datumIterator) {
      ResponseList.ResponseListBuilder builder = ResponseList.newBuilder();

      while (true) {
        Datum datum;
        try {
          datum = datumIterator.next();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          continue;
        }
        if (datum == null) {
          break;
        }

        if (Arrays.equals(datum.getKeys(), new String[] {"invalid-key"})) {
            builder.addResponse(
                    Response.responseFailure(datum.getId() + processedIdSuffix, "error message"));
        } else if (Arrays.equals(datum.getKeys(), new String[] {"fallback-key"})) {
            builder.addResponse(
                    Response.responseFallback(datum.getId() + processedIdSuffix));
        } else if (Arrays.equals(datum.getKeys(), new String[] {"onsuccess-key"})) {
            builder.addResponse(
                    Response.responseOnSuccess(datum.getId() + processedIdSuffix, (OnSuccessMessage) null));
        } else if (Arrays.equals(datum.getKeys(), new String[] {"serve-key"})) {
            builder.addResponse(
                    Response.responseServe(datum.getId() + processedIdSuffix, "serve message".getBytes()));
        } else {
              builder.addResponse(Response.responseOK(datum.getId() + processedIdSuffix));
        }
      }

      return builder.build();
    }
  }
}
