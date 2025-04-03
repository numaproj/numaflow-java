package io.numaproj.numaflow.errors;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;


public class PersistCriticalErrorTest {

    @Test
    public void test_writes_error_details_to_new_file() throws Exception {
        String errorCode = "404";
        String errorMessage = "Not Found";
        String errorDetails = "The requested resource was not found.";
        String baseDir = "/tmp/test-success";
        try{
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
        }catch(Exception e){
            fail("Exception occurred while writing error details to the file: " + e.getMessage());
        }
    }

    @Test
    public void test_handles_null_or_empty_error_code() throws Exception {
        String errorCode = null;
        String errorMessage = "Internal Server Error";
        String errorDetails = "An unexpected error occurred.";
        String baseDir = "/tmp/test-errors";
        try{
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
        }catch(Exception e){
            fail("Exception occurred while writing error details to the file: " + e.getMessage());
        }
    }
}
