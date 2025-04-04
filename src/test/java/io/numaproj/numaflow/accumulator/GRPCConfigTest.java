package io.numaproj.numaflow.accumulator;

import org.junit.Assert;
import org.junit.Test;

public class GRPCConfigTest {

    @Test
    public void testDefaultGrpcConfig() {
        GRPCConfig grpcConfig = GRPCConfig.defaultGrpcConfig();
        Assert.assertNotNull(grpcConfig);
        Assert.assertEquals(
                Constants.DEFAULT_SERVER_INFO_FILE_PATH,
                grpcConfig.getInfoFilePath());
        Assert.assertEquals(Constants.DEFAULT_MESSAGE_SIZE, grpcConfig.getMaxMessageSize());
        Assert.assertEquals(Constants.DEFAULT_SOCKET_PATH, grpcConfig.getSocketPath());
        Assert.assertEquals(Constants.DEFAULT_PORT, grpcConfig.getPort());
        Assert.assertTrue(grpcConfig.isLocal());
    }

    @Test
    public void testNewBuilder() {
        int port = 8001;
        String socketPath = "test-socket-path";
        int maxMessageSize = 2000;
        String infoFilePath = "test-info-file-path";
        GRPCConfig grpcConfig = GRPCConfig.newBuilder()
                .socketPath(socketPath)
                .maxMessageSize(maxMessageSize)
                .infoFilePath(infoFilePath)
                .port(port)
                .isLocal(false)
                .build();
        Assert.assertNotNull(grpcConfig);
        Assert.assertEquals(socketPath, grpcConfig.getSocketPath());
        Assert.assertEquals(maxMessageSize, grpcConfig.getMaxMessageSize());
        Assert.assertEquals(infoFilePath, grpcConfig.getInfoFilePath());
        Assert.assertEquals(port, grpcConfig.getPort());
        Assert.assertFalse(grpcConfig.isLocal());
    }
}

