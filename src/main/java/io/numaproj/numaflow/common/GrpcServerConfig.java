package io.numaproj.numaflow.common;

import io.numaproj.numaflow.function.Function;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GrpcServerConfig {
    private String socketPath;
    private int maxMessageSize;

    public GrpcServerConfig(String socketPath, int maxMessageSize) {
        this.socketPath = socketPath;
        this.maxMessageSize = maxMessageSize;
    }
}
