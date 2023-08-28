package io.numaproj.numaflow.reducer;

import com.google.common.annotations.VisibleForTesting;
import io.numaproj.numaflow.info.ServerInfoAccessor;
import lombok.Getter;

/**
 * ReduceGRPCConfig is used to provide configurations for gRPC server.
 */
@Getter
public class GRPCConfig {
    private final String socketPath;
    private final int maxMessageSize;
    private String infoFilePath;

    /**
     * Constructor to create Config with message size.
     * @param maxMessageSize max payload size for gRPC server.
     */
    public GRPCConfig(int maxMessageSize) {
        this.socketPath = Constants.SOCKET_PATH;
        this.maxMessageSize = maxMessageSize;
        this.infoFilePath = ServerInfoAccessor.DEFAULT_SERVER_INFO_FILE_PATH;
    }

    @VisibleForTesting
    public void setInfoFilePath(String infoFilePath) {
        this.infoFilePath = infoFilePath;
    }
}
