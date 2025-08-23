package io.numaproj.numaflow.examples.sink.forkjoin;

import io.numaproj.numaflow.sinker.Datum;
import io.numaproj.numaflow.sinker.DatumIterator;
import io.numaproj.numaflow.sinker.Response;
import io.numaproj.numaflow.sinker.ResponseList;
import io.numaproj.numaflow.sinker.Server;
import io.numaproj.numaflow.sinker.Sinker;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * ConcurrentSink demonstrates concurrent processing of incoming messages using ThreadPoolExecutor.
 * This example shows how to process messages in parallel using a thread pool for
 * CPU-intensive operations where parallel processing can improve performance.
 *
 * Key features:
 * - Uses ThreadPoolExecutor for parallel execution
 * - Processes each message independently in parallel
 * - Demonstrates concurrent data transformation
 * - Handles exceptions gracefully in parallel processing
 * - Shows how to aggregate results from multiple threads
 */
@Slf4j
public class ConcurrentSink extends Sinker {

    private static final int DEFAULT_THREAD_POOL_SIZE = 10;

    private final ThreadPoolExecutor threadPool;

    public ConcurrentSink() {
        this(DEFAULT_THREAD_POOL_SIZE);
    }

    public ConcurrentSink(int threadPoolSize) {
        this.threadPool = new ThreadPoolExecutor(
                threadPoolSize,
                threadPoolSize,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactory() {
                    private int counter = 0;
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "ConcurrentSink-Worker-" + (++counter));
                        t.setDaemon(true);
                        return t;
                    }
                }
        );
    }

    public static void main(String[] args) throws Exception {
        ConcurrentSink concurrentSink = new ConcurrentSink(4);

        Server server = new Server(concurrentSink);
        server.start();
        server.awaitTermination();
    }

    @Override
    public ResponseList processMessages(DatumIterator datumIterator) {
        log.info("Starting concurrent processing with thread pool size: {}",
                threadPool.getCorePoolSize());
        
        List<Datum> messages = new ArrayList<>();
        while (true) {
            Datum datum = null;
            try {
                datum = datumIterator.next();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                continue;
            }
            if (datum == null) {
                break;
            }
            messages.add(datum);
        }

        log.info("Collected {} messages for concurrent processing", messages.size());

        if (messages.isEmpty()) {
            return ResponseList.newBuilder().build();
        }

        List<Response> allResponses = processInParallel(messages);

        log.info("Completed concurrent processing, generated {} responses", allResponses.size());

        ResponseList.ResponseListBuilder responseListBuilder = ResponseList.newBuilder();
        for (Response response : allResponses) {
            responseListBuilder.addResponse(response);
        }

        return responseListBuilder.build();
    }

    /**
     * Processes messages in parallel using ThreadPoolExecutor.
     * Each message is processed independently in a separate thread.
     */
    private List<Response> processInParallel(List<Datum> messages) {
        List<Future<Response>> futures = new ArrayList<>();

        for (Datum message : messages) {
            Future<Response> future = threadPool.submit(new MessageProcessingTask(message));
            futures.add(future);
        }

        List<Response> allResponses = new ArrayList<>();
        for (Future<Response> future : futures) {
            try {
                Response response = future.get(30, TimeUnit.SECONDS);
                allResponses.add(response);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted while waiting for message processing", e);
            } catch (ExecutionException e) {
                log.error("Error during message processing", e.getCause());
            } catch (TimeoutException e) {
                log.error("Timeout waiting for message processing", e);
                future.cancel(true);
            }
        }

        return allResponses;
    }

    /**
     * Task that processes a single message in a thread.
     * This is where the actual CPU-intensive work would be done.
     */
    private static class MessageProcessingTask implements Callable<Response> {
        private final Datum datum;

        public MessageProcessingTask(Datum datum) {
            this.datum = datum;
        }

        @Override
        public Response call() {
            try {
                String message = new String(datum.getValue());
                String processedMessage = processMessage(message);

                log.debug("Processed message {} -> {}", message, processedMessage);
                return Response.responseOK(datum.getId());

            } catch (Exception e) {
                log.error("Error processing message with ID: {}", datum.getId(), e);
                return Response.responseFailure(datum.getId(), e.getMessage());
            }
        }

        /**
         * Simulates CPU-intensive message processing.
         * In a real-world scenario, this could be data transformation, validation,
         * encryption, compression, or any other compute-intensive operation.
         */
        private String processMessage(String message) {
            StringBuilder processed = new StringBuilder();
            
            processed.append("PROCESSED[")
                    .append(new StringBuilder(message).reverse())
                    .append("]-")
                    .append(Thread.currentThread().getName())
                    .append("-")
                    .append(System.currentTimeMillis() % 1000);
            
            for (int i = 0; i < 100; i++) {
                Math.sqrt(i * message.hashCode());
            }
            
            return processed.toString();
        }
    }

    /**
     * Shutdown the thread pool gracefully.
     * This should be called when the sink is no longer needed.
     */
    public void shutdown() {
        log.info("Shutting down concurrent sink thread pool");
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Get current thread pool statistics for monitoring.
     */
    public String getThreadPoolStats() {
        return String.format("ThreadPool[active=%d, completed=%d, queued=%d, pool=%d/%d]",
                threadPool.getActiveCount(),
                threadPool.getCompletedTaskCount(),
                threadPool.getQueue().size(),
                threadPool.getPoolSize(),
                threadPool.getMaximumPoolSize());
    }
}
