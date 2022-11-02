package io.numaproj.numaflow.sink;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.common.GrpcServerConfig;
import io.numaproj.numaflow.sink.v1.Udsink;
import io.numaproj.numaflow.sink.v1.UserDefinedSinkGrpc;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class SinkServerTest {
  private final static String processedIdSuffix = "-id-processed";
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private final Function<Udsink.Datum[], Response[]> testSinkFn =
      (datumList) -> new Response[]{new Response(datumList[0].getId() + processedIdSuffix, true, "")};
  private SinkServer server;
  private ManagedChannel inProcessChannel;

  @Before
  public void setUp() throws Exception {
    String serverName = InProcessServerBuilder.generateName();
    server = new SinkServer(InProcessServerBuilder.forName(serverName).directExecutor(), new GrpcServerConfig(Sink.SOCKET_PATH, Sink.DEFAULT_MESSAGE_SIZE));
    server.registerSinker(new SinkFunc(testSinkFn)).start();
    inProcessChannel = grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build());
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  @Test
  public void sinker() {
    ByteString inValue = ByteString.copyFromUtf8("invalue");
    Udsink.DatumList inDatumList = Udsink.DatumList.newBuilder()
        .addElements(Udsink.Datum.newBuilder().setId("inid").setValue(inValue).build())
        .build();

    String expectedId = "inid" + processedIdSuffix;
    // for now just hardcoding these and the function returned values
    boolean expectedSuccess = true;
    String expectedErr = "";

    var stub = UserDefinedSinkGrpc.newBlockingStub(inProcessChannel);
    var actualDatumList = stub.sinkFn(inDatumList);

    var actualResponses = actualDatumList.getResponsesList();
    assertEquals(1, actualResponses.size());
    assertEquals(expectedId, actualResponses.get(0).getId());
    assertTrue(actualResponses.get(0).getSuccess());
    assertEquals(expectedErr, actualResponses.get(0).getErrMsg());
  }
}
