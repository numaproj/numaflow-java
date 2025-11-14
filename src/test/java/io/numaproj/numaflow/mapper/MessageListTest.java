package io.numaproj.numaflow.mapper;

import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertArrayEquals;

public class MessageListTest {
    @Test
    public void testMessageList() {
        Message defaultMessage = new Message("asdf".getBytes());
        ArrayList<Message> messageList = new ArrayList<>();
        messageList.add(defaultMessage);

        MessageList messageList1 = new MessageList.MessageListBuilder()
                .addMessages(messageList)
                .addMessage(defaultMessage)
                .build();

        messageList.add(defaultMessage);

        assertArrayEquals(messageList1.getMessages().toArray(), messageList.toArray());
    }
}
