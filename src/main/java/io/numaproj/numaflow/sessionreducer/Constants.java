package io.numaproj.numaflow.sessionreducer;

class Constants {
  public static final int DEFAULT_MESSAGE_SIZE = 1024 * 1024 * 64;
  public static final String DEFAULT_SOCKET_PATH = "/var/run/numaflow/sessionreduce.sock";
  public static final String DEFAULT_SERVER_INFO_FILE_PATH =
      "/var/run/numaflow/sessionreducer-server-info";
  public static final int DEFAULT_PORT = 50051;
  public static final String EOF = "EOF";
  public static final String SUCCESS = "SUCCESS";
  public static final String DEFAULT_HOST = "localhost";
  public static final String DELIMITER = ":";

  // Private constructor to prevent instantiation
  private Constants() {
    throw new IllegalStateException("Utility class 'Constants' should not be instantiated");
  }
}
