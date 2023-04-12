package io.numaproj.numaflow.common;

import io.numaproj.numaflow.function.FunctionConstants;
import io.numaproj.numaflow.info.ServerInfoConstants;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class GRPCServerConfig {
    private String socketPath;
    private int maxMessageSize;
    private String infoFilePath;

    public GRPCServerConfig() {
        this.socketPath = FunctionConstants.DEFAULT_SOCKET_PATH;
        this.maxMessageSize = FunctionConstants.DEFAULT_MESSAGE_SIZE;
        this.infoFilePath = ServerInfoConstants.DEFAULT_SERVER_INFO_FILE_PATH;
    }
}
