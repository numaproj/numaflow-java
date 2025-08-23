package io.numaproj.numaflow.examples.sink.forkjoin;

import io.numaproj.numaflow.sinker.Response;
import io.numaproj.numaflow.sinker.ResponseList;
import io.numaproj.numaflow.sinker.SinkerTestKit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for ConcurrentSink to verify concurrent processing functionality.
 */
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

    @Nested
    @DisplayName("Basic Functionality Tests")
    class BasicFunctionalityTests {

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
    }

    @Nested
    @DisplayName("Concurrency Tests")
    class ConcurrencyTests {

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
        @DisplayName("Should handle large dataset efficiently")
        void testLargeDataset() {
            int messageCount = 100;
            
            for (int i = 0; i < messageCount; i++) {
                testIterator.addDatum(createTestDatum("id-" + i, "large-dataset-message-" + i));
            }
            
            long startTime = System.currentTimeMillis();
            ResponseList responseList = concurrentSink.processMessages(testIterator);
            long endTime = System.currentTimeMillis();
            
            assertNotNull(responseList);
            assertEquals(messageCount, responseList.getResponses().size());
            
            // Verify all responses are successful
            long successCount = responseList.getResponses().stream()
                    .mapToLong(response -> response.getSuccess() ? 1 : 0)
                    .sum();
            assertEquals(messageCount, successCount);
            
            // Log processing time for performance analysis
            System.out.printf("Processed %d messages in %d ms%n", messageCount, endTime - startTime);
        }

        @Test
        @DisplayName("Should handle different thread pool sizes")
        void testDifferentThreadPoolSizes() {
            int[] threadPoolSizes = {1, 2, 4, 8};
            int messageCount = 20;

            for (int threadPoolSize : threadPoolSizes) {
                SinkerTestKit.TestListIterator iterator = new SinkerTestKit.TestListIterator();
                ConcurrentSink sink = new ConcurrentSink(threadPoolSize);

                try {
                    for (int i = 0; i < messageCount; i++) {
                        iterator.addDatum(createTestDatum("id-" + i, "thread-test-" + i));
                    }

                    ResponseList responseList = sink.processMessages(iterator);

                    assertNotNull(responseList, "Response list should not be null for thread pool size " + threadPoolSize);
                    assertEquals(messageCount, responseList.getResponses().size(),
                            "Should process all messages for thread pool size " + threadPoolSize);

                    for (Response response : responseList.getResponses()) {
                        assertTrue(response.getSuccess(),
                                "All responses should be successful for thread pool size " + threadPoolSize);
                    }
                } finally {
                    sink.shutdown();
                }
            }
        }
    }

    @Nested
    @DisplayName("Error Handling Tests")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Should handle null values gracefully")
        void testNullValues() {
            testIterator.addDatum(createTestDatum("id-1", null));
            testIterator.addDatum(createTestDatum("id-2", "valid-message"));
            
            ResponseList responseList = concurrentSink.processMessages(testIterator);
            
            assertNotNull(responseList);
            assertEquals(2, responseList.getResponses().size());
            
            // At least one response should be successful (the valid message)
            long successCount = responseList.getResponses().stream()
                    .mapToLong(response -> response.getSuccess() ? 1 : 0)
                    .sum();
            assertTrue(successCount >= 1);
        }

        @Test
        @DisplayName("Should handle mixed valid and invalid data")
        void testMixedData() {
            testIterator.addDatum(createTestDatum("id-1", "valid-message-1"));
            testIterator.addDatum(createTestDatum("id-2", ""));
            testIterator.addDatum(createTestDatum("id-3", "valid-message-3"));
            testIterator.addDatum(createTestDatum("id-4", "valid-message-4"));
            
            ResponseList responseList = concurrentSink.processMessages(testIterator);
            
            assertNotNull(responseList);
            assertEquals(4, responseList.getResponses().size());
            
            // Verify that we get responses for all messages
            for (Response response : responseList.getResponses()) {
                assertNotNull(response.getId());
                assertTrue(response.getId().startsWith("id-"));
            }
        }
    }

    @Nested
    @DisplayName("Performance Tests")
    class PerformanceTests {

        @Test
        @DisplayName("Should demonstrate concurrent processing benefits")
        void testConcurrentProcessingPerformance() {
            int messageCount = 50;
            
            // Prepare test data
            SinkerTestKit.TestListIterator sequentialIterator = new SinkerTestKit.TestListIterator();
            SinkerTestKit.TestListIterator concurrentIterator = new SinkerTestKit.TestListIterator();
            
            for (int i = 0; i < messageCount; i++) {
                String message = "performance-test-message-" + i;
                sequentialIterator.addDatum(createTestDatum("seq-id-" + i, message));
                concurrentIterator.addDatum(createTestDatum("con-id-" + i, message));
            }
            
            // Test with single thread (effectively sequential)
            ConcurrentSink sequentialSink = new ConcurrentSink(1);
            long sequentialStart = System.currentTimeMillis();
            ResponseList sequentialResult = sequentialSink.processMessages(sequentialIterator);
            long sequentialTime = System.currentTimeMillis() - sequentialStart;
            sequentialSink.shutdown();

            // Test with multiple threads (concurrent processing)
            ConcurrentSink concurrentSink = new ConcurrentSink(4);
            long concurrentStart = System.currentTimeMillis();
            ResponseList concurrentResult = concurrentSink.processMessages(concurrentIterator);
            long concurrentTime = System.currentTimeMillis() - concurrentStart;
            concurrentSink.shutdown();
            
            // Verify both produce same number of results
            assertEquals(messageCount, sequentialResult.getResponses().size());
            assertEquals(messageCount, concurrentResult.getResponses().size());
            
            System.out.printf("Sequential processing: %d ms%n", sequentialTime);
            System.out.printf("Concurrent processing: %d ms%n", concurrentTime);
            
            // Note: In unit tests, the overhead might make concurrent processing slower
            // for small datasets, but this test demonstrates the framework works
        }

        @Test
        @DisplayName("Should handle thread pool statistics")
        void testThreadPoolStatistics() {
            String initialStats = concurrentSink.getThreadPoolStats();
            assertNotNull(initialStats);
            assertTrue(initialStats.contains("ThreadPool"));
            
            // Process some messages to generate activity
            for (int i = 0; i < 10; i++) {
                testIterator.addDatum(createTestDatum("stats-id-" + i, "stats-message-" + i));
            }
            
            ResponseList responseList = concurrentSink.processMessages(testIterator);
            assertNotNull(responseList);
            assertEquals(10, responseList.getResponses().size());
            
            String finalStats = concurrentSink.getThreadPoolStats();
            assertNotNull(finalStats);
            assertTrue(finalStats.contains("ThreadPool"));
        }
    }

    private SinkerTestKit.TestDatum createTestDatum(String id, String value) {
        return SinkerTestKit.TestDatum.builder()
                .id(id)
                .value(value != null ? value.getBytes() : new byte[0])
                .build();
    }
}
