package io.numaproj.numaflow.mapstreamer;

class Constants {
  public static final int DEFAULT_MESSAGE_SIZE = 1024 * 1024 * 64;
  public static final String DEFAULT_SOCKET_PATH = "/var/run/numaflow/mapstream.sock";
  public static final String DEFAULT_SERVER_INFO_FILE_PATH = "/var/run/numaflow/mapper-server-info";
  public static final int DEFAULT_PORT = 50051;
  public static final String DEFAULT_HOST = "localhost";
  public static final String MAP_MODE_KEY = "MAP_MODE";
  public static final String MAP_MODE = "stream-map";

  // Private constructor to prevent instantiation
  private Constants() {
    throw new IllegalStateException("Utility class 'Constants' should not be instantiated");
  }
}
