package io.numaproj.numaflow.sourcer;

import io.numaproj.numaflow.info.ServerInfoAccessor;
import org.junit.Assert;
import org.junit.Test;

public class GRPCConfigTest {

    @Test
    public void testDefaultGrpcConfig() {
        GRPCConfig grpcConfig = GRPCConfig.defaultGrpcConfig();
        Assert.assertNotNull(grpcConfig);
        Assert.assertEquals(
                ServerInfoAccessor.DEFAULT_SERVER_INFO_FILE_PATH,
                grpcConfig.getInfoFilePath());
        Assert.assertEquals(Constants.DEFAULT_MESSAGE_SIZE, grpcConfig.getMaxMessageSize());
        Assert.assertEquals(Constants.DEFAULT_SOCKET_PATH, grpcConfig.getSocketPath());
    }

    @Test
    public void testNewBuilder() {
        String socketPath = "test-socket-path";
        int maxMessageSize = 2000;
        String infoFilePath = "test-info-file-path";
        GRPCConfig grpcConfig = GRPCConfig.newBuilder()
                .socketPath(socketPath)
                .maxMessageSize(maxMessageSize)
                .infoFilePath(infoFilePath)
                .build();
        Assert.assertNotNull(grpcConfig);
        Assert.assertEquals(socketPath, grpcConfig.getSocketPath());
        Assert.assertEquals(maxMessageSize, grpcConfig.getMaxMessageSize());
        Assert.assertEquals(infoFilePath, grpcConfig.getInfoFilePath());
    }
}

