package io.numaproj.numaflow.sideinput;

import lombok.Builder;
import lombok.Getter;

/**
 * GRPCConfig is used to provide configurations for gRPC server.
 */
@Getter
@Builder(builderMethodName = "newBuilder")
public class GRPCConfig {
    private String socketPath;
    private int maxMessageSize;

    /**
     * Static method to create default GRPCConfig.
     */
    static GRPCConfig defaultGrpcConfig() {
        return GRPCConfig.newBuilder()
                .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
                .socketPath(Constants.DEFAULT_SOCKET_PATH).build();
    }
}
