package io.numaproj.numaflow.errors;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Test;

@Slf4j
public class PersistCriticalErrorTest {

    @After
    public void resetIsPersisted() {
        // Reset the isPersisted flag to its default value (false) after each test
        PersistCriticalError.setIsPersisted(false);
    }

    @Test
    public void test_writes_error_details_to_new_file() {
        String errorCode = "404";
        String errorMessage = "Not Found";
        String errorDetails = "The requested resource was not found.";
        String baseDir = "/tmp/test-success";

        try {
            PersistCriticalError.persistCriticalErrorToFile(errorCode, errorMessage, errorDetails, baseDir);
            Path containerDirPath = Paths.get(baseDir, PersistCriticalError.CONTAINER_TYPE);
            Path jsonFilePath = Files.list(containerDirPath)
                    .filter(path -> path.toString().endsWith(".json"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No .json file found in the directory"));

            // Read the contents of the file
            String fileContent = Files.readString(jsonFilePath);
            // Assert that the file contains the expected values
            assertTrue(fileContent.contains(errorCode));
            assertTrue(fileContent.contains(errorMessage));
            assertTrue(fileContent.contains(errorDetails));
        } catch (Exception e) {
            fail("Exception occurred while writing error details to the file: " + e.getMessage());
        }
    }

    @Test
    public void test_handles_null_or_empty_error_code() {
        String errorCode = null;
        String errorMessage = "Internal Server Error";
        String errorDetails = "An unexpected error occurred.";
        String baseDir = "/tmp/test-errors";

        try {
            PersistCriticalError.persistCriticalErrorToFile(errorCode, errorMessage, errorDetails, baseDir);
            Path containerDirPath = Paths.get(baseDir, PersistCriticalError.CONTAINER_TYPE);
            Path jsonFilePath = Files.list(containerDirPath)
                    .filter(path -> path.toString().endsWith(".json"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("No .json file found in the directory"));

            // Read the contents of the file
            String fileContent = Files.readString(jsonFilePath);
            // Assert that the file contains the expected values
            assertTrue(fileContent.contains("Internal error"));
            assertTrue(fileContent.contains(errorMessage));
            assertTrue(fileContent.contains(errorDetails));
        } catch (Exception e) {
            fail("Exception occurred while writing error details to the file: " + e.getMessage());
        }
    }

    @Test
    public void test_persistCriticalError_when_isPersisted_is_true_with_threads() {
        // Set isPersisted to true before calling persistCriticalError
        PersistCriticalError.setIsPersisted(true);

        String errorCode = "500";
        String errorMessage = "Critical Error";
        String errorDetails = "A critical error occurred.";

        // Number of threads to simulate concurrent calls
        int numberOfThreads = 5;

        // Create a thread pool
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);

        // Runnable task to call persistCriticalError
        Runnable task = () -> {
            try {
                PersistCriticalError.persistCriticalError(errorCode, errorMessage, errorDetails);
                fail("Expected IllegalStateException to be thrown, but it was not.");
            } catch (IllegalStateException e) {
                // Assert that the exception message is as expected
                assertTrue(e.getMessage().contains("Persist critical error function has already been executed."));
            } catch (Exception e) {
                fail("Unexpected exception occurred: " + e.getMessage());
            }
        };

        // Submit tasks to the executor
        for (int i = 0; i < numberOfThreads; i++) {
            executorService.submit(task);
        }

        // Shut down the executor
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)) {
                fail("Executor service did not terminate in the expected time.");
            }
        } catch (InterruptedException e) {
            fail("Runtime exception occurred: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
}
