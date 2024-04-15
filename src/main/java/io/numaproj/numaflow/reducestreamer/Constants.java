package io.numaproj.numaflow.reducestreamer;

class Constants {
    public static final int DEFAULT_MESSAGE_SIZE = 1024 * 1024 * 64;

    public static final String DEFAULT_SOCKET_PATH = "/var/run/numaflow/reducestream.sock";

    public static final String DEFAULT_SERVER_INFO_FILE_PATH = "/var/run/numaflow/reducestreamer-server-info";

    public static final int DEFAULT_PORT = 50051;

    public static final String DEFAULT_HOST = "localhost";

    public static final String EOF = "EOF";

    public static final String SUCCESS = "SUCCESS";

    public static final String DELIMITER = ":";
}
