package io.numaproj.numaflow.shared;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionUtils {
    /**
     * Formalized exception error strings
     */
    public static final String ERR_SOURCE_EXCEPTION = "UDF_EXECUTION_ERROR(source)";
    public static final String ERR_TRANSFORMER_EXCEPTION = "UDF_EXECUTION_ERROR(transformer)";

    /**
     * Converts the stack trace of an exception into a String.
     *
     * @param e the exception to extract the stack trace from
     * @return the stack trace as a String
     */
    public static String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
}
