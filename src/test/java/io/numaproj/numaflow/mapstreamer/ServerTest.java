package io.numaproj.numaflow.mapstreamer;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.mapstream.v1.MapStreamGrpc;
import io.numaproj.numaflow.mapstream.v1.Mapstream;
import io.numaproj.numaflow.shared.Constants;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ServerTest {
    private final static String PROCESSED_KEY_SUFFIX = "-key-processed";
    private final static String PROCESSED_VALUE_SUFFIX = "-value-processed";

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private Server server;
    private ManagedChannel inProcessChannel;

    @Before
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();

        GRPCConfig grpcServerConfig = new GRPCConfig(Constants.DEFAULT_MESSAGE_SIZE);
        grpcServerConfig.setInfoFilePath("/tmp/numaflow-test-server-info");
        server = new Server(new TestMapStreamFn(),
                grpcServerConfig);

        server.setServerBuilder(InProcessServerBuilder.forName(serverName)
                .directExecutor());

        server.start();

        inProcessChannel = grpcCleanup.register(InProcessChannelBuilder
                .forName(serverName)
                .directExecutor()
                .build());
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void TestMapStreamer() {
        ByteString inValue = ByteString.copyFromUtf8("invalue");
        Mapstream.MapStreamRequest inDatum = Mapstream.MapStreamRequest
                .newBuilder()
                .addAllKeys(List.of("test-map-stream-key"))
                .setValue(inValue)
                .build();

        String[] expectedKeys = new String[]{"test-map-stream-key" + PROCESSED_KEY_SUFFIX};
        String[] expectedTags = new String[]{"test-tag"};
        ByteString expectedValue = ByteString.copyFromUtf8("invalue" + PROCESSED_VALUE_SUFFIX);

        var stub = MapStreamGrpc.newBlockingStub(inProcessChannel);

        var actualDatumList = stub
                .mapStreamFn(inDatum);


        int count = 0;
        while (actualDatumList.hasNext()) {
            Mapstream.MapStreamResponse d = actualDatumList.next();
            assertEquals(
                    expectedKeys,
                    d.getResult().getKeysList().toArray(new String[0]));
            assertEquals(expectedValue, d.getResult().getValue());
            assertEquals(
                    expectedTags,
                    d.getResult().getTagsList().toArray(new String[0]));
            count++;
        }
        assertEquals(10, count);
    }

    private static class TestMapStreamFn extends MapStreamer {
        @Override
        public void processMessage(String[] keys, Datum datum, StreamObserver<Mapstream.MapStreamResponse> streamObserver) {
            String[] updatedKeys = Arrays
                    .stream(keys)
                    .map(c -> c + PROCESSED_KEY_SUFFIX)
                    .toArray(String[]::new);
            for (int i = 0; i < 10; i++) {
                Message msg = new Message(
                                (new String(datum.getValue())
                                        + PROCESSED_VALUE_SUFFIX).getBytes(),
                                updatedKeys,
                                new String[]{"test-tag"});
                onNext(msg, streamObserver);
            }
        }
    }
}
