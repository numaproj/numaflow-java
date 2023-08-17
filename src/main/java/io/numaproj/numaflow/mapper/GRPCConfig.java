package io.numaproj.numaflow.mapper;

import com.google.common.annotations.VisibleForTesting;
import io.numaproj.numaflow.info.ServerInfoConstants;
import io.numaproj.numaflow.shared.Constants;
import lombok.Getter;

/**
 * GRPCConfig is used to provide configurations for map gRPC server.
 */
@Getter
public class GRPCConfig {
    private final String socketPath;
    private final int maxMessageSize;
    private String infoFilePath;

    /**
     * Constructor to create Config with message size.
     * @param maxMessageSize max payload size for map gRPC server.
     */
    public GRPCConfig(int maxMessageSize) {
        this.socketPath = Constants.DEFAULT_SOCKET_PATH;
        this.maxMessageSize = maxMessageSize;
        this.infoFilePath = ServerInfoConstants.DEFAULT_SERVER_INFO_FILE_PATH;
    }

    @VisibleForTesting
    public void setInfoFilePath(String infoFilePath) {
        this.infoFilePath = infoFilePath;
    }
}
