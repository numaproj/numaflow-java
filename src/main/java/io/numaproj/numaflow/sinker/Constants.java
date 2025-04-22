package io.numaproj.numaflow.sinker;

class Constants {
  public static final String DEFAULT_SOCKET_PATH = "/var/run/numaflow/sink.sock";
  public static final String DEFAULT_FB_SINK_SOCKET_PATH = "/var/run/numaflow/fb-sink.sock";
  public static final String DEFAULT_SERVER_INFO_FILE_PATH = "/var/run/numaflow/sinker-server-info";
  public static final String DEFAULT_FB_SERVER_INFO_FILE_PATH =
      "/var/run/numaflow/fb-sinker-server-info";
  public static final int DEFAULT_MESSAGE_SIZE = 1024 * 1024 * 64;
  public static final int DEFAULT_PORT = 50051;
  public static final String DEFAULT_HOST = "localhost";
  public static final String ENV_UD_CONTAINER_TYPE = "NUMAFLOW_UD_CONTAINER_TYPE";
  public static final String UD_CONTAINER_FALLBACK_SINK = "fb-udsink";

  // Private constructor to prevent instantiation
  private Constants() {
    throw new IllegalStateException("Utility class 'Constants' should not be instantiated");
  }
}
