package io.numaproj.numaflow.sinker;

import lombok.Builder;
import lombok.Getter;

/**
 * GRPCConfig is used to provide configurations for gRPC server.
 */
@Getter
@Builder(builderMethodName = "newBuilder")
public class GRPCConfig {
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
        String containerType = System.getenv(Constants.ENV_UD_CONTAINER_TYPE);
        String socketPath = Constants.DEFAULT_SOCKET_PATH;
        String infoFilePath = Constants.DEFAULT_SERVER_INFO_FILE_PATH;

        // if containerType is fb-udsink then we need to use fb sink socket path and info file path
        if (Constants.UD_CONTAINER_FALLBACK_SINK.equals(containerType)) {
            socketPath = Constants.DEFAULT_FB_SINK_SOCKET_PATH;
            infoFilePath = Constants.DEFAULT_FB_SERVER_INFO_FILE_PATH;
        }
        return GRPCConfig.newBuilder()
                .infoFilePath(infoFilePath)
                .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
                .isLocal(containerType
                        == null) // if ENV_UD_CONTAINER_TYPE is not set, then we are not running using numaflow
                .socketPath(socketPath)
                .build();
    }
}
