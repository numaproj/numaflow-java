package io.numaproj.numaflow.common;

import io.numaproj.numaflow.function.Function;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class GrpcServerConfig {
    private String socketPath;
    private int maxMessageSize;

    public GrpcServerConfig(int maxMessageSize) {
        this(Function.SOCKET_PATH, maxMessageSize);
    }

    public GrpcServerConfig(String socketPath) {
        this(socketPath, Function.DEFAULT_MESSAGE_SIZE);
    }
}
