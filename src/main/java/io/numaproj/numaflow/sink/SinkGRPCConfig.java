package io.numaproj.numaflow.sink;

import com.google.common.annotations.VisibleForTesting;
import io.numaproj.numaflow.info.ServerInfoConstants;
import lombok.Getter;

/**
 * SinkGRPCConfig is used to provide configurations for sink gRPC server.
 */
@Getter
class SinkGRPCConfig {
    private final String socketPath;
    private final int maxMessageSize;
    private String infoFilePath;

    /**
     * Constructor to create Config with message size.
     * @param maxMessageSize max payload size for sink gRPC server.
     */
    public SinkGRPCConfig(int maxMessageSize) {
        this.socketPath = SinkConstants.DEFAULT_SOCKET_PATH;
        this.maxMessageSize = maxMessageSize;
        this.infoFilePath = ServerInfoConstants.DEFAULT_SERVER_INFO_FILE_PATH;
    }

    @VisibleForTesting
    public void setInfoFilePath(String infoFilePath) {
        this.infoFilePath = infoFilePath;
    }
}
