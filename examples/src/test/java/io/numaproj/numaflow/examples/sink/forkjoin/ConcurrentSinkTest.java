package io.numaproj.numaflow.examples.sink.forkjoin;

import io.numaproj.numaflow.sinker.Response;
import io.numaproj.numaflow.sinker.ResponseList;
import io.numaproj.numaflow.sinker.SinkerTestKit;
import io.numaproj.numaflow.sinker.Datum;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.mock;


import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

/**
 * Comprehensive test suite for ConcurrentSink to verify concurrent processing functionality.
 */
@RunWith(MockitoJUnitRunner.class)
public class ConcurrentSinkTest {

    private ConcurrentSink concurrentSink;
    private SinkerTestKit.TestListIterator testIterator;

    @BeforeEach
    void setUp() {
        concurrentSink = new ConcurrentSink();
        testIterator = new SinkerTestKit.TestListIterator();
    }

    @AfterEach
    void tearDown() {
        if (concurrentSink != null) {
            concurrentSink.shutdown();
        }
    }

    @Test
    @DisplayName("Should process empty message list")
    void testEmptyMessageList() {
        ResponseList responseList = concurrentSink.processMessages(testIterator);
        
        assertNotNull(responseList);
        assertEquals(0, responseList.getResponses().size());
    }

    @Test
    @DisplayName("Should process single message")
    void testSingleMessage() {
        testIterator.addDatum(createTestDatum("id-1", "test-message"));
        
        ResponseList responseList = concurrentSink.processMessages(testIterator);
        
        assertNotNull(responseList);
        assertEquals(1, responseList.getResponses().size());
        
        Response response = responseList.getResponses().get(0);
        assertTrue(response.getSuccess());
        assertEquals("id-1", response.getId());
    }

    @Test
    @DisplayName("Should process multiple messages")
    void testMultipleMessages() {
        int messageCount = 15;

        for (int i = 0; i < messageCount; i++) {
            testIterator.addDatum(createTestDatum("id-" + i, "message-" + i));
        }

        ResponseList responseList = concurrentSink.processMessages(testIterator);

        assertNotNull(responseList);
        assertEquals(messageCount, responseList.getResponses().size());

        for (Response response : responseList.getResponses()) {
            assertTrue(response.getSuccess());
        }
    }

    @Test
    @DisplayName("Should handle concurrent processing with custom configuration")
    void testCustomConfiguration() {
        int threadPoolSize = 2;
        ConcurrentSink customSink = new ConcurrentSink(threadPoolSize);

        try {
            int messageCount = 15;
            SinkerTestKit.TestListIterator iterator = new SinkerTestKit.TestListIterator();

            for (int i = 0; i < messageCount; i++) {
                iterator.addDatum(createTestDatum("id-" + i, "message-" + i));
            }

            ResponseList responseList = customSink.processMessages(iterator);

            assertNotNull(responseList);
            assertEquals(messageCount, responseList.getResponses().size());

            for (Response response : responseList.getResponses()) {
                assertTrue(response.getSuccess());
            }

            // Verify thread pool stats
            String stats = customSink.getThreadPoolStats();
            assertNotNull(stats);
            assertTrue(stats.contains("ThreadPool"));

        } finally {
            customSink.shutdown();
        }
    }

    @Test
    @DisplayName("Should handle null values gracefully")
    void testNullValues() {
        testIterator.addDatum(createTestDatum("id-1", null));
        testIterator.addDatum(createTestDatum("id-2", "valid-message"));
        
        ResponseList responseList = concurrentSink.processMessages(testIterator);
        
        assertNotNull(responseList);
        assertEquals(2, responseList.getResponses().size());
        
        long successCount = responseList.getResponses().stream()
                .mapToLong(response -> response.getSuccess() ? 1 : 0)
                .sum();
        assertEquals(2, successCount);
    }

    @Test
    @DisplayName("Should handle errors gracefully")
    void testErrors() {
        Datum mockDatum = mock(Datum.class);
        testIterator.addDatum(mockDatum);
        testIterator.addDatum(mockDatum);

        when(mockDatum.getValue()).thenThrow(new RuntimeException("some exception happened"));
        
        ResponseList responseList = concurrentSink.processMessages(testIterator);
        
        assertNotNull(responseList);
        assertEquals(2, responseList.getResponses().size());
        
        long errorCount = responseList.getResponses().stream()
                .mapToLong(response -> response.getSuccess() ? 0 : 1)
                .sum();
        assertEquals(2, errorCount);
    }

    private SinkerTestKit.TestDatum createTestDatum(String id, String value) {
        return SinkerTestKit.TestDatum.builder()
                .id(id)
                .value(value != null ? value.getBytes() : new byte[0])
                .build();
    }
}
