package io.numaproj.numaflow.common;

import io.numaproj.numaflow.function.Function;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class GRPCServerConfig {
    private String socketPath;
    private int maxMessageSize;

    public GRPCServerConfig(int maxMessageSize) {
        this(Function.SOCKET_PATH, maxMessageSize);
    }

    public GRPCServerConfig(String socketPath) {
        this(socketPath, Function.DEFAULT_MESSAGE_SIZE);
    }
}
