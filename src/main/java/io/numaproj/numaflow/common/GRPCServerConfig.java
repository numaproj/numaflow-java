package io.numaproj.numaflow.common;

import com.google.common.annotations.VisibleForTesting;
import io.numaproj.numaflow.function.FunctionConstants;
import io.numaproj.numaflow.info.ServerInfoConstants;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
public class GRPCServerConfig {
    private final String socketPath;
    private int maxMessageSize;
    private String infoFilePath;

    public GRPCServerConfig(String socketPath) {
        this.socketPath = socketPath;
        this.maxMessageSize = FunctionConstants.DEFAULT_MESSAGE_SIZE;
        this.infoFilePath = ServerInfoConstants.DEFAULT_SERVER_INFO_FILE_PATH;
    }

    public GRPCServerConfig(int maxMessageSize) {
        this.socketPath = FunctionConstants.DEFAULT_SOCKET_PATH;
        this.maxMessageSize = FunctionConstants.DEFAULT_MESSAGE_SIZE;
        this.infoFilePath = ServerInfoConstants.DEFAULT_SERVER_INFO_FILE_PATH;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    @VisibleForTesting
    public void setInfoFilePath(String infoFilePath) {
        this.infoFilePath = infoFilePath;
    }
}
