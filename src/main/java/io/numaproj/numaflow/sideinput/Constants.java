package io.numaproj.numaflow.sideinput;

public class Constants {
    public static final String SIDE_INPUT_DIR = "/var/numaflow/side-inputs";
    static final String DEFAULT_SOCKET_PATH = "/var/run/numaflow/sideinput.sock";

    static final String DEFAULT_SERVER_INFO_FILE_PATH = "/var/run/numaflow/sideinput-server-info";
    static int DEFAULT_MESSAGE_SIZE = 1024 * 1024 * 64;

    static int DEFAULT_PORT = 50051;
}
