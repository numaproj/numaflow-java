package io.numaproj.numaflow.servingstore;

class Constants {
  public static final int DEFAULT_MESSAGE_SIZE = 1024 * 1024 * 64;
  public static final String DEFAULT_SOCKET_PATH = "/var/run/numaflow/serving.sock";
  public static final String DEFAULT_SERVER_INFO_FILE_PATH =
      "/var/run/numaflow/serving-server-info";
  public static final int DEFAULT_PORT = 50051;
  public static final String DEFAULT_HOST = "localhost";

  // Private constructor to prevent instantiation
  private Constants() {
    throw new IllegalStateException("Utility class 'Constants' should not be instantiated");
  }
}
