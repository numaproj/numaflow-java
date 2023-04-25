package io.numaproj.numaflow.function;

import com.google.common.annotations.VisibleForTesting;
import io.numaproj.numaflow.info.ServerInfoConstants;
import lombok.Getter;

@Getter
public class FunctionGRPCConfig {
    private final String socketPath;
    private final int maxMessageSize;
    private String infoFilePath;

    public FunctionGRPCConfig(int maxMessageSize) {
        this.socketPath = FunctionConstants.DEFAULT_SOCKET_PATH;
        this.maxMessageSize = maxMessageSize;
        this.infoFilePath = ServerInfoConstants.DEFAULT_SERVER_INFO_FILE_PATH;
    }

    @VisibleForTesting
    public void setInfoFilePath(String infoFilePath) {
        this.infoFilePath = infoFilePath;
    }
}
