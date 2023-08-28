package io.numaproj.numaflow.shared;

import io.grpc.Context;
import io.grpc.ServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.numaproj.numaflow.info.ServerInfoAccessor;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class GrpcServerUtilsTest {

    @Test
    public void testGetChannelTypeClass() {
        Class<? extends ServerChannel> channelTypeClass = GrpcServerUtils.getChannelTypeClass();
        Assert.assertNotNull(channelTypeClass);
    }

    @Test
    public void testCreateEventLoopGroup() {
        int threads = 4;
        String name = "test-group";
        EventLoopGroup eventLoopGroup = GrpcServerUtils.createEventLoopGroup(threads, name);
        Assert.assertNotNull(eventLoopGroup);
    }

    @Test
    public void testWriteServerInfo() throws Exception {
        ServerInfoAccessor mockAccessor = Mockito.mock(ServerInfoAccessor.class);
        Mockito.when(mockAccessor.getSDKVersion()).thenReturn("1.0.0");
        GrpcServerUtils.writeServerInfo(mockAccessor, null, "infoFilePath");
        Mockito.verify(mockAccessor, Mockito.times(1)).write(Mockito.any(), Mockito.eq("infoFilePath"));
    }

    @Test
    public void testCreateServerBuilder() {
        ServerBuilder<?> serverBuilder = GrpcServerUtils.createServerBuilder("socketPath", 1000);
        Assert.assertNotNull(serverBuilder);
    }

    @Test
    public void testWindowStartTime() {
        Context.Key<String> windowStartTime = GrpcServerUtils.WINDOW_START_TIME;
        Assert.assertNotNull(windowStartTime);
    }

    @Test
    public void testWindowEndTime() {
        Context.Key<String> windowEndTime = GrpcServerUtils.WINDOW_END_TIME;
        Assert.assertNotNull(windowEndTime);
    }
}
