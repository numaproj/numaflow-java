package io.numaproj.numaflow.accumulator;

import io.numaproj.numaflow.shared.GrpcConfigRetriever;
import lombok.Builder;
import lombok.Getter;

/**
 * GRPCConfig is used to provide configurations for map gRPC server.
 */
@Getter
@Builder(builderMethodName = "newBuilder")
public class GRPCConfig implements GrpcConfigRetriever {
    @Builder.Default
    private String socketPath = Constants.DEFAULT_SOCKET_PATH;

    @Builder.Default
    private int maxMessageSize = Constants.DEFAULT_MESSAGE_SIZE;

    @Builder.Default
    private String infoFilePath = Constants.DEFAULT_SERVER_INFO_FILE_PATH;

    @Builder.Default
    private int port = Constants.DEFAULT_PORT;

    private boolean isLocal;

    /**
     * Static method to create default GRPCConfig.
     */
    static GRPCConfig defaultGrpcConfig() {
        return GRPCConfig.newBuilder()
                .infoFilePath(Constants.DEFAULT_SERVER_INFO_FILE_PATH)
                .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
                .isLocal(System.getenv("NUMAFLOW_POD")
                        == null) // if NUMAFLOW_POD is not set, then we are not running
                // using numaflow
                .socketPath(Constants.DEFAULT_SOCKET_PATH).build();
    }
}
