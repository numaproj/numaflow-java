package io.numaproj.numaflow.shared;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionUtils {
    /**
     * Formalized exception error strings
     */
    public static final String ERR_SOURCE_EXCEPTION = "UDF_EXECUTION_ERROR(source)";
    public static final String ERR_TRANSFORMER_EXCEPTION = "UDF_EXECUTION_ERROR(transformer)";
    public static final String ERR_SINK_EXCEPTION = "UDF_EXECUTION_ERROR(sink)";
    public static final String ERR_MAP_STREAM_EXCEPTION = "UDF_EXECUTION_ERROR(mapstream)";
    public static final String ERR_MAP_EXCEPTION = "UDF_EXECUTION_ERROR(map)";
    public static final String ERR_BATCH_MAP_EXCEPTION = "UDF_EXECUTION_ERROR(batchmap)";


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
