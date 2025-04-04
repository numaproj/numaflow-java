package io.numaproj.numaflow.errors;

import io.numaproj.numaflow.shared.ExceptionUtils;
import lombok.extern.slf4j.Slf4j;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;


/** The PersistCriticalError class provides functionality to persist critical errors to a file
* in a runtime directory. This is useful for logging critical errors in a structured format
* for debugging and monitoring purposes. The class ensures that the error persistence operation
* is executed only once during the application's runtime.
*/
@Slf4j
public class PersistCriticalError {

    private static final String DEFAULT_RUNTIME_APPLICATION_ERRORS_PATH = "/var/numaflow/runtime/application-errors";
    private static final String CURRENT_FILE = "current-udf.json";
    private static final String INTERNAL_ERROR = "Internal error";
    private static final String UNKNOWN_CONTAINER = "unknown-container";
    static final String CONTAINER_TYPE = System.getenv(ExceptionUtils.ENV_UD_CONTAINER_TYPE) != null
            ? System.getenv(ExceptionUtils.ENV_UD_CONTAINER_TYPE)
            : UNKNOWN_CONTAINER;

    private static final AtomicBoolean isPersisted = new AtomicBoolean(false);

    /**
     * Persists a critical error to a file. Ensures the method is executed only once.
     *
     * @param errorCode    the error code
     * @param errorMessage the error message
     * @param errorDetails additional error details
     * @throws IllegalStateException if the method has already been executed
     */
    public static synchronized void persistCriticalError(String errorCode, String errorMessage, String errorDetails) throws Exception {
        // Check if the function has already been executed
        if (!isPersisted.compareAndSet(false, true)) {
            throw new IllegalStateException("Persist critical error function has already been executed.");
        }
        try{
            persistCriticalErrorToFile(errorCode, errorMessage, errorDetails, DEFAULT_RUNTIME_APPLICATION_ERRORS_PATH);
        } catch(IOException e){
            log.error("Error occurred while persisting critical error: {}", e.getMessage(), e);
        }
        
    }

    /**
     * Writes the critical error details to a file.
     *
     * @param errorCode    the error code
     * @param errorMessage the error message
     * @param errorDetails additional error details
     * @throws IOException if an error occurs while writing to the file
     */
    static void persistCriticalErrorToFile(String errorCode, String errorMessage, String errorDetails, String baseDir) throws IOException {

        Path baseDirPath = createDirectory(Paths.get(baseDir));
        Path containerDirPath = createDirectory(baseDirPath.resolve(CONTAINER_TYPE));

        errorCode = (errorCode == null || errorCode.isEmpty()) ? INTERNAL_ERROR : errorCode;
        long timestamp = Instant.now().getEpochSecond();

        // Create an ErrorEntry object
        ErrorEntry errorEntry = new ErrorEntry(CONTAINER_TYPE, timestamp, errorCode, errorMessage, errorDetails);
        // Convert the ErrorEntry object to JSON
        String errorEntryJson = errorEntry.toJson();

        Path currentFilePath = containerDirPath.resolve(CURRENT_FILE);
        // Create or overwrite an existing file for writing the error entry
        Files.writeString(currentFilePath, errorEntryJson, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        // Rename the current file to include the timestamp
        Path finalFilePath = containerDirPath.resolve(String.format("%d-udf.json", timestamp));
        Files.move(currentFilePath, finalFilePath, StandardCopyOption.REPLACE_EXISTING);
    }

     /**
     * Creates a directory with read,write,execute permissions for all if it does not already exist.
     *
     * @param dirPath the path of the directory to create
     * @return the path of the created directory
     * @throws IOException if an error occurs while creating the directory
     */
    private static Path createDirectory(Path dirPath) throws IOException {
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
            Files.setPosixFilePermissions(dirPath, PosixFilePermissions.fromString("rwxrwxrwx"));
        }
        return dirPath;
    }

    /**
     * Private static class representing an error entry.
     */
    private static class ErrorEntry {
        @JsonProperty("container")
        private final String container;
        @JsonProperty("timestamp")
        private final long timestamp;
        @JsonProperty("code")
        private final String code;
        @JsonProperty("message")
        private final String message;
        @JsonProperty("details")
        private final String details;

        public ErrorEntry(String container, long timestamp, String code, String message, String details) {
            this.container = container;
            this.timestamp = timestamp;
            this.code = code;
            this.message = message;
            this.details = details;
        }

        /**
         * Converts the ErrorEntry object to a JSON string.
         *
         * @return JSON representation of the error entry
         * @throws IOException if an error occurs during serialization
         */
        public String toJson() throws IOException {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                return objectMapper.writeValueAsString(this);
            } catch (JsonProcessingException e) {
                throw new IOException("Failed to convert ErrorEntry to JSON", e);
            }
        }
    }
}
