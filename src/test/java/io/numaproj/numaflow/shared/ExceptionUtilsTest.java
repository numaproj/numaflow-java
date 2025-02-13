package io.numaproj.numaflow.shared;

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class ExceptionUtilsTest {

    @Test
    public void testGetStackTrace_NullException() {
        String result = ExceptionUtils.getStackTrace(null);
        assertEquals("No exception provided.", result);
    }

    @Test
    public void testGetStackTrace_ValidException() {
        Exception exception = new Exception("Test exception");
        String result = ExceptionUtils.getStackTrace(exception);
        assertTrue(result.contains("Test exception"));
        assertTrue(result.contains("ExceptionUtilsTest.testGetStackTrace_ValidException"));
    }

}
