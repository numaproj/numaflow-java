package io.numaproj.numaflow.function;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GrpcServerConfig {
    private String socketPath;
    private int maxMessageSize;

    public GrpcServerConfig() {
        this.maxMessageSize = Function.DEFAULT_MESSAGE_SIZE;
        this.socketPath = Function.SOCKET_PATH;
    }
}
