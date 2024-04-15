package io.numaproj.numaflow.examples.source.simple;

import io.numaproj.numaflow.sourcer.Message;
import io.numaproj.numaflow.sourcer.Offset;
import io.numaproj.numaflow.sourcer.SourcerTestKit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;


public class SimpleSourceTest {

    @Test
    public void test_ReadAndAck() {
        SimpleSource simpleSource = new SimpleSource();
        SourcerTestKit.TestListBasedObserver testObserver = new SourcerTestKit.TestListBasedObserver();

        // Read 2 messages
        SourcerTestKit.TestReadRequest readRequest = SourcerTestKit.TestReadRequest.builder()
                .count(2).timeout(Duration.ofMillis(1000)).build();
        simpleSource.read(readRequest, testObserver);
        Assertions.assertEquals(2, testObserver.getMessages().size());

        // Try reading 4 more messages
        // Since the previous batch didn't get acked, the data source shouldn't allow us to read more messages
        // We should get 0 messages, meaning the observer only holds the previous 2 messages
        SourcerTestKit.TestReadRequest readRequest2 = SourcerTestKit.TestReadRequest.builder()
                .count(2).timeout(Duration.ofMillis(1000)).build();
        simpleSource.read(readRequest2, testObserver);
        Assertions.assertEquals(2, testObserver.getMessages().size());

        // Ack the first batch

        ArrayList<Offset> offsets = new ArrayList<>();
        // iterate over the testObserver messages and get the offset
        for (Message message : testObserver.getMessages()) {
            offsets.add(message.getOffset());
        }
        SourcerTestKit.TestAckRequest ackRequest = SourcerTestKit.TestAckRequest.builder()
                .offsets(offsets).build();
        simpleSource.ack(ackRequest);

        // Try reading 6 more messages
        // Since the previous batch got acked, the data source should allow us to read more messages
        // We should get 6 more messages - total of 2+6=8
        SourcerTestKit.TestReadRequest readRequest3 = SourcerTestKit.TestReadRequest.builder()
                .count(6).timeout(Duration.ofMillis(1000)).build();
        simpleSource.read(readRequest3, testObserver);
        Assertions.assertEquals(8, testObserver.getMessages().size());
    }

    @Test
    public void testPending() {
        SimpleSource simpleSource = new SimpleSource();
        // simple source getPending always returns 0.
        Assertions.assertEquals(0, simpleSource.getPending());
    }

}

