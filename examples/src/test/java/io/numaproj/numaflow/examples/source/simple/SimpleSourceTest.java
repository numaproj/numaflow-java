package io.numaproj.numaflow.examples.source.simple;

import com.google.common.primitives.Longs;
import io.numaproj.numaflow.sourcer.AckRequest;
import io.numaproj.numaflow.sourcer.Message;
import io.numaproj.numaflow.sourcer.Offset;
import io.numaproj.numaflow.sourcer.OutputObserver;
import io.numaproj.numaflow.sourcer.ReadRequest;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SimpleSourceTest {

    static class TestObserver implements OutputObserver {
        List<Message> messages = new ArrayList<>();

        @Override
        public void send(Message message) {
            messages.add(message);
        }
    }

    @Test
    public void testRead() {
        ReadRequest readRequest = Mockito.mock(ReadRequest.class);
        Mockito.when(readRequest.getCount()).thenReturn(100L);
        Mockito.when(readRequest.getTimeout()).thenReturn(Duration.ofMillis(1000));

        SimpleSource simpleSource = new SimpleSource();
        TestObserver testObserver = new TestObserver();
        simpleSource.read(readRequest, testObserver);

        assertEquals(100, testObserver.messages.size());

        for (int i = 0; i < 100; i++) {
            Message message = testObserver.messages.get(i);
            assertEquals(String.valueOf(i), new String(message.getValue()));
            assertNotNull(message.getHeaders().get("x-txn-id"));
        }
    }

    @Test
    public void readAckAndCheckPending() {
        SimpleSource simpleSource = new SimpleSource();
        // Mock ReadRequest because it is an interface
        ReadRequest firstRequest = Mockito.mock(ReadRequest.class);
        Mockito.when(firstRequest.getCount()).thenReturn(100L);
        Mockito.when(firstRequest.getTimeout()).thenReturn(Duration.ofMillis(1000));

        // Create a Test Observer
        TestObserver testObserver = new TestObserver();

        // Read 100 messages
        simpleSource.read(firstRequest, testObserver);

        // Acknowledge the messages
        AckRequest ackRequest = Mockito.mock(AckRequest.class);
        ArrayList<Offset> offsets = new ArrayList<>();
        // iterate over the testObserver messages and get the offset
        for (Message message : testObserver.messages) {
            offsets.add(message.getOffset());
        }
        Mockito.when(ackRequest.getOffsets()).thenReturn(offsets);
        simpleSource.ack(ackRequest);

        assertEquals(100, testObserver.messages.size()); // 100 messages read
        assertEquals(0, simpleSource.getPending()); // 0 messages remaining
        assertEquals(0, simpleSource.getMessages().size()); // all messages acknowledged
    }

    @Test
    public void testAck() {
        AckRequest ackRequest = Mockito.mock(AckRequest.class);
        Offset offset = new Offset(Longs.toByteArray(1));
        Mockito.when(ackRequest.getOffsets()).thenReturn(List.of(new Offset[]{offset}));

        SimpleSource simpleSource = new SimpleSource();
        simpleSource.ack(ackRequest);

        assertEquals(0, simpleSource.getPending());
    }
}

