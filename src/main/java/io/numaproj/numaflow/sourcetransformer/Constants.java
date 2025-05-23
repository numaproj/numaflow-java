package io.numaproj.numaflow.sourcetransformer;

class Constants {
  public static final String DEFAULT_SOCKET_PATH = "/var/run/numaflow/sourcetransform.sock";
  public static final String DEFAULT_SERVER_INFO_FILE_PATH =
      "/var/run/numaflow/sourcetransformer-server-info";
  public static final String DEFAULT_HOST = "localhost";
  public static final String EOF = "EOF";
  public static int DEFAULT_MESSAGE_SIZE = 1024 * 1024 * 64;
  public static int DEFAULT_PORT = 50051;

  // Private constructor to prevent instantiation
  private Constants() {
    throw new IllegalStateException("Utility class 'Constants' should not be instantiated");
  }
}
