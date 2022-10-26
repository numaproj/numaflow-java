package io.numaproj.numaflow.function;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.function.v1.Udfunction;
import io.numaproj.numaflow.function.v1.UserDefinedFunctionGrpc;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.function.BiFunction;

import static io.numaproj.numaflow.function.Function.DATUM_KEY;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class FunctionServerTest {
  private final static String processedKeySuffix = "-key-processed";
  private final static String processedValueSuffix = "-value-processed";
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private final BiFunction<String, Udfunction.Datum, Message[]> testMapFn =
      (key, datum) -> new Message[]{new Message(key + processedKeySuffix, (new String(datum.getValue().toByteArray()) + processedValueSuffix).getBytes())};
  private FunctionServer server;
  private ManagedChannel inProcessChannel;

  @Before
  public void setUp() throws Exception {
    String serverName = InProcessServerBuilder.generateName();
    server = new FunctionServer(InProcessServerBuilder.forName(serverName).directExecutor(), null);
    server.registerMapper(new MapFunc(testMapFn)).start();
    inProcessChannel = grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build());
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  @Test
  public void mapper() {
    ByteString inValue = ByteString.copyFromUtf8("invalue");
    Udfunction.Datum inDatum = Udfunction.Datum.newBuilder().setKey("not-my-key").setValue(inValue).build();

    String expectedKey = "inkey" + processedKeySuffix;
    ByteString expectedValue = ByteString.copyFromUtf8("invalue" + processedValueSuffix);

    Metadata metadata = new Metadata();
    metadata.put(Metadata.Key.of(DATUM_KEY, Metadata.ASCII_STRING_MARSHALLER), "inkey");

    var stub = UserDefinedFunctionGrpc.newBlockingStub(inProcessChannel);
    var actualDatumList = stub
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
        .mapFn(inDatum);

    assertEquals(1, actualDatumList.getElementsCount());
    assertEquals(expectedKey, actualDatumList.getElements(0).getKey());
    assertEquals(expectedValue, actualDatumList.getElements(0).getValue());
  }
}
