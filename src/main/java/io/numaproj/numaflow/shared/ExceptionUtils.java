package io.numaproj.numaflow.shared;

import com.google.protobuf.Any;
import com.google.rpc.Code;
import com.google.rpc.DebugInfo;
import io.grpc.Status;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;

public class ExceptionUtils {

  /** UD Container Type Environment Variable */
  public static final String ENV_UD_CONTAINER_TYPE = "NUMAFLOW_UD_CONTAINER_TYPE";

  public static final String CONTAINER_NAME = System.getenv(ENV_UD_CONTAINER_TYPE);

  // Private constructor to prevent instantiation
  private ExceptionUtils() {
    throw new IllegalStateException("Utility class 'ExceptionUtils' should not be instantiated");
  }

  /**
   * Converts the stack trace of an exception into a String.
   *
   * @param t the exception to extract the stack trace from
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
    return "UDF_EXECUTION_ERROR("
        + Objects.requireNonNullElse(CONTAINER_NAME, "unknown-container")
        + ")";
  }

    /**
     * Builds rpc status from the user's exception.
     * 
     * @param exception encountered in user's code.
     * @return the status constructed using the exception.
     */
  public static com.google.rpc.Status buildStatusFromUserException(Exception exception) {
       return com.google.rpc.Status.newBuilder()
              .setCode(Code.INTERNAL.getNumber())
              .setMessage(ExceptionUtils.getExceptionErrorString() + ": " + (exception.getMessage() != null ? exception.getMessage() : ""))
              .addDetails(Any.pack(DebugInfo.newBuilder()
                      .setDetail(ExceptionUtils.getStackTrace(exception))
                      .build()))
              .build();
  }
}
