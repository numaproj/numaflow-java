package io.numaproj.numaflow.batchmapper;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.numaproj.numaflow.batchmap.v1.BatchMapGrpc;
import io.numaproj.numaflow.batchmap.v1.Batchmap;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

@Slf4j
public class ServerTest {
    private final static String PROCESSED_KEY_SUFFIX = "-key-processed";
    private final static String PROCESSED_VALUE_SUFFIX = "-value-processed";
    private final static String key = "key";

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
    private Server server;
    private ManagedChannel inProcessChannel;

    @Before
    public void setUp() throws Exception {
        String serverName = InProcessServerBuilder.generateName();

        GRPCConfig grpcServerConfig = GRPCConfig.newBuilder()
                .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
                .socketPath(Constants.DEFAULT_SOCKET_PATH)
                .infoFilePath("/tmp/numaflow-test-server-info)")
                .build();

        server = new Server(
                new TestMapFn(),
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
    public void testBatchMapHappyPath() {
        BatchMapOutputStreamObserver outputStreamObserver = new BatchMapOutputStreamObserver();
        StreamObserver<Batchmap.BatchMapRequest> inputStreamObserver = BatchMapGrpc
                .newStub(inProcessChannel)
                .batchMapFn(outputStreamObserver);

        for (int i =1; i <= 10; i++) {
            String uuid = Integer.toString(i);
            String message = i + "," + String.valueOf(i+10);
            Batchmap.BatchMapRequest request = Batchmap.BatchMapRequest.newBuilder()
                    .setValue(ByteString.copyFromUtf8(message))
                    .addKeys(key)
                    .setId(uuid)
                    .build();
            log.info("Sending request with ID : {} and msg: {}", uuid, message);
            inputStreamObserver.onNext(request);
        }

        inputStreamObserver.onCompleted();
        while (outputStreamObserver.resultDatum.get().size() != 10);
        List<Batchmap.BatchMapResponse> result = outputStreamObserver.resultDatum.get();
        assertEquals(10, result.size());
        for (int i=0; i < 10; i++) {
            assertEquals(result.get(i).getId(),String.valueOf(i+1));
        }
    }

    private static class TestMapFn extends BatchMapper {
        @Override
        public BatchResponses processMessage(DatumIterator datumStream) {
            BatchResponses batchResponses = new BatchResponses();
            while (true) {
                Datum datum = null;
                try {
                    datum = datumStream.next();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    continue;
                }
                if (datum == null) {
                    break;
                }
                String msg = new String(datum.getValue());
                String[] strs = msg.split(",");
                BatchResponse batchResponse = new BatchResponse(datum.getId());
                log.info("Processing message with id: {}", datum.getId());
                for (String str : strs) {
                    batchResponse.append(new Message(str.getBytes()));
                }
                batchResponses.append(batchResponse);
            }
            log.info("Returning respose list with size {}", batchResponses.getItems().size());
            return batchResponses;
        }
    }
}
