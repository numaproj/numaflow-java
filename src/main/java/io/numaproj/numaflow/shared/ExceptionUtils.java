package io.numaproj.numaflow.shared;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;

public class ExceptionUtils {

    /** 
     * UD Container Type Environment Variable 
    */
    public static final String ENV_UD_CONTAINER_TYPE = "NUMAFLOW_UD_CONTAINER_TYPE";
    public static final String CONTAINER_NAME = System.getenv(ENV_UD_CONTAINER_TYPE);

    /**
     * Converts the stack trace of an exception into a String.
     *
     * @param t the exception to extract the stack trace from
     *
     * @return the stack trace as a String
     */
    public static String getStackTrace(Throwable t) {
        if (t == null) {
            return "No exception provided.";
        }
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    /**
     * Returns a formalized exception error string.
     *
     * @return the formalized exception error string
     */
    public static String getExceptionErrorString() {
        return "UDF_EXECUTION_ERROR(" + Objects.requireNonNullElse(CONTAINER_NAME, "unknown-container") + ")";
    }
}
