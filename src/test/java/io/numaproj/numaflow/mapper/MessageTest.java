package io.numaproj.numaflow.mapper;

import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;

public class MessageTest {
    @Test
    public void testMessage() {
        Message message1 = new Message("asdf".getBytes());
        assertArrayEquals("asdf".getBytes(), message1.getValue());
        Message message2 = new Message("asdf".getBytes(), new String[]{"key1"});
        assertArrayEquals("asdf".getBytes(), message2.getValue());
        assertArrayEquals(new String[]{"key1"}, message2.getKeys());
        Message message3 = new Message(null, null, null);
        assertArrayEquals(null, message3.getValue());
        Message message4 = Message.toDrop();
        assertArrayEquals(new byte[0], message4.getValue());
        assertArrayEquals(null, message4.getKeys());
        String[] drop_tags = {"U+005C__DROP__"};
        assertArrayEquals(drop_tags, message4.getTags());
    }
}
