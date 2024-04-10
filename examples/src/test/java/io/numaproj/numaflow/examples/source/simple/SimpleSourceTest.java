package io.numaproj.numaflow.examples.source.simple;

import io.numaproj.numaflow.sourcer.AckRequest;
import io.numaproj.numaflow.sourcer.Message;
import io.numaproj.numaflow.sourcer.Offset;
import io.numaproj.numaflow.sourcer.OutputObserver;
import io.numaproj.numaflow.sourcer.ReadRequest;
import lombok.Data;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimpleSourceTest {

    @Test
    public void test_ReadAndAck() {
        SimpleSource simpleSource = new SimpleSource();
        TestObserver testObserver = new TestObserver();

        // Read 2 messages
        ReadRequest readRequest = Mockito.mock(ReadRequest.class);
        Mockito.when(readRequest.getCount()).thenReturn(2L);
        Mockito.when(readRequest.getTimeout()).thenReturn(Duration.ofMillis(1000));
        simpleSource.read(readRequest, testObserver);
        assertEquals(2, testObserver.messages.size());

        // Try reading 4 more messages
        // Since the previous batch didn't get acked, the data source shouldn't allow us to read more messages
        // We should get 0 messages, meaning the observer only holds the previous 2 messages
        ReadRequest readRequest2 = Mockito.mock(ReadRequest.class);
        Mockito.when(readRequest2.getCount()).thenReturn(2L);
        Mockito.when(readRequest2.getTimeout()).thenReturn(Duration.ofMillis(1000));
        simpleSource.read(readRequest2, testObserver);
        assertEquals(2, testObserver.messages.size());

        // Ack the first batch
        AckRequest ackRequest = Mockito.mock(AckRequest.class);
        ArrayList<Offset> offsets = new ArrayList<>();
        // iterate over the testObserver messages and get the offset
        for (Message message : testObserver.messages) {
            offsets.add(message.getOffset());
        }
        Mockito.when(ackRequest.getOffsets()).thenReturn(offsets);
        simpleSource.ack(ackRequest);

        // Try reading 6 more messages
        // Since the previous batch got acked, the data source should allow us to read more messages
        // We should get 6 more messages - total of 2+6=8
        ReadRequest readRequest3 = Mockito.mock(ReadRequest.class);
        Mockito.when(readRequest3.getCount()).thenReturn(6L);
        Mockito.when(readRequest3.getTimeout()).thenReturn(Duration.ofMillis(1000));
        simpleSource.read(readRequest3, testObserver);
        assertEquals(8, testObserver.messages.size());
    }

    @Test
    public void testPending() {
        SimpleSource simpleSource = new SimpleSource();
        // simple source getPending always returns 0.
        assertEquals(0, simpleSource.getPending());
    }

    @Data
    static class TestObserver implements OutputObserver {
        List<Message> messages = new ArrayList<>();

        @Override
        public void send(Message message) {
            messages.add(message);
        }
    }
}

