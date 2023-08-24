package io.numaproj.numaflow.sideinput;

import io.numaproj.numaflow.shared.Constants;
import lombok.Getter;

/**
 * GRPCConfig is used to provide configurations for map gRPC server.
 */
@Getter
public class GRPCConfig {
    private final String socketPath;
    private final int maxMessageSize;

    /**
     * Constructor to create Config with message size.
     * @param maxMessageSize max payload size for map gRPC server.
     */
    public GRPCConfig(int maxMessageSize) {
        this.socketPath = Constants.MAP_SOCKET_PATH;
        this.maxMessageSize = maxMessageSize;
    }

}
