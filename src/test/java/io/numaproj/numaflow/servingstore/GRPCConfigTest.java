package io.numaproj.numaflow.servingstore;

public class GRPCConfigTest {

    @org.junit.Test
    public void testDefaultGrpcConfig() {
        GRPCConfig grpcConfig = GRPCConfig.defaultGrpcConfig();
        org.junit.Assert.assertNotNull(grpcConfig);
        org.junit.Assert.assertEquals(
                Constants.DEFAULT_SERVER_INFO_FILE_PATH,
                grpcConfig.getInfoFilePath());
        org.junit.Assert.assertEquals(Constants.DEFAULT_MESSAGE_SIZE, grpcConfig.getMaxMessageSize());
        org.junit.Assert.assertEquals(Constants.DEFAULT_SOCKET_PATH, grpcConfig.getSocketPath());
        org.junit.Assert.assertEquals(Constants.DEFAULT_PORT, grpcConfig.getPort());
        org.junit.Assert.assertTrue(grpcConfig.isLocal());
    }

    @org.junit.Test
    public void testNewBuilder() {
        String socketPath = "test-socket-path";
        int maxMessageSize = 2000;
        String infoFilePath = "test-info-file-path";
        int port = 8001;
        GRPCConfig grpcConfig = GRPCConfig.newBuilder()
                .socketPath(socketPath)
                .maxMessageSize(maxMessageSize)
                .infoFilePath(infoFilePath)
                .port(port)
                .isLocal(false)
                .build();
        org.junit.Assert.assertNotNull(grpcConfig);
        org.junit.Assert.assertEquals(socketPath, grpcConfig.getSocketPath());
        org.junit.Assert.assertEquals(maxMessageSize, grpcConfig.getMaxMessageSize());
        org.junit.Assert.assertEquals(infoFilePath, grpcConfig.getInfoFilePath());
        org.junit.Assert.assertEquals(port, grpcConfig.getPort());
        org.junit.Assert.assertFalse(grpcConfig.isLocal());
    }
}
