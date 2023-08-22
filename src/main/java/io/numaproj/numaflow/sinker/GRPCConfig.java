package io.numaproj.numaflow.sinker;

import com.google.common.annotations.VisibleForTesting;
import io.numaproj.numaflow.shared.Constants;
import lombok.Getter;

/**
 * SinkGRPCConfig is used to provide configurations for sink gRPC server.
 */
@Getter
class GRPCConfig {
    private final String socketPath;
    private final int maxMessageSize;
    private String infoFilePath;

    /**
     * Constructor to create Config with message size.
     * @param maxMessageSize max payload size for sink gRPC server.
     */
    public GRPCConfig(int maxMessageSize) {
        this.socketPath = Constants.SINK_SOCKET_PATH;
        this.maxMessageSize = maxMessageSize;
        this.infoFilePath = Constants.DEFAULT_SERVER_INFO_FILE_PATH;
    }

    @VisibleForTesting
    public void setInfoFilePath(String infoFilePath) {
        this.infoFilePath = infoFilePath;
    }
}
