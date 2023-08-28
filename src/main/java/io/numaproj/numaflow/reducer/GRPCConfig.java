package io.numaproj.numaflow.reducer;

import io.numaproj.numaflow.info.ServerInfoAccessor;
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
    private String infoFilePath;

    /**
     * Static method to create default GRPCConfig.
     */
    static GRPCConfig defaultGrpcConfig() {
        return GRPCConfig.newBuilder()
                .infoFilePath(ServerInfoAccessor.DEFAULT_SERVER_INFO_FILE_PATH)
                .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
                .socketPath(Constants.DEFAULT_SOCKET_PATH).build();
    }
}
