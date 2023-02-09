package io.numaproj.numaflow.common;

import io.numaproj.numaflow.function.Function;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GrpcServerConfig {
    private String socketPath;
    private int maxMessageSize;

    public GrpcServerConfig(int maxMessageSize) {
        this(Function.SOCKET_PATH, maxMessageSize);
    }

    public GrpcServerConfig(String socketPath) {
        this(socketPath, Function.DEFAULT_MESSAGE_SIZE);
    }

    public GrpcServerConfig(String socketPath, int maxMessageSize) {
        this.socketPath = socketPath;
        this.maxMessageSize = maxMessageSize;
    }
}
