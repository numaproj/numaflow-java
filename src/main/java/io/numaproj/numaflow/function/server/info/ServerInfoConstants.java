package io.numaproj.numaflow.function.server.info;

/**
 * ServerInfoConstants defines common server information like language, protocol and information file path etc.
 * Please exercise cautions when updating the values below because the exact same values are defined in other Numaflow SDKs
 * to form a contract between server and clients.
 */
public class ServerInfoConstants {
    public static final String DEFAULT_SERVER_INFO_FILE_PATH = "/var/run/numaflow/server-info";

    // TODO - ENUM?
    public static final String LANGUAGE_GO = "go";
    public static final String LANGUAGE_PYTHON = "python";
    public static final String LANGUAGE_JAVA = "java";

    public static final String UDS_PROTOCOL = "uds";
    public static final String TCP_PROTOCOL = "tcp";
}
