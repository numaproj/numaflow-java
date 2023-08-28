package io.numaproj.numaflow.sideinput;

import org.junit.Assert;
import org.junit.Test;

public class GRPCConfigTest {

    @Test
    public void testDefaultGrpcConfig() {
        GRPCConfig grpcConfig = GRPCConfig.defaultGrpcConfig();
        Assert.assertNotNull(grpcConfig);
        Assert.assertEquals(Constants.DEFAULT_MESSAGE_SIZE, grpcConfig.getMaxMessageSize());
        Assert.assertEquals(Constants.DEFAULT_SOCKET_PATH, grpcConfig.getSocketPath());
    }

    @Test
    public void testNewBuilder() {
        String socketPath = "test-socket-path";
        int maxMessageSize = 2000;
        GRPCConfig grpcConfig = GRPCConfig.newBuilder()
                .socketPath(socketPath)
                .maxMessageSize(maxMessageSize)
                .build();
        Assert.assertNotNull(grpcConfig);
        Assert.assertEquals(socketPath, grpcConfig.getSocketPath());
        Assert.assertEquals(maxMessageSize, grpcConfig.getMaxMessageSize());
    }
}
