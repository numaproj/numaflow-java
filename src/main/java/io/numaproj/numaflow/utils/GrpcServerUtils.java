package io.numaproj.numaflow.utils;

import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.numaproj.numaflow.info.Language;
import io.numaproj.numaflow.info.Protocol;
import io.numaproj.numaflow.info.ServerInfo;
import io.numaproj.numaflow.info.ServerInfoAccessor;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

/**
 * GrpcServerUtils is the utility class for netty server channel.
 */
@Slf4j
public class GrpcServerUtils {


    /*
        * Get the server socket channel class based on the availability of epoll and kqueue.
     */
    public static Class<? extends ServerChannel> getChannelTypeClass() {
        if (KQueue.isAvailable()) {
            return KQueueServerDomainSocketChannel.class;
        }
        return EpollServerDomainSocketChannel.class;
    }

    /*
        * Get the event loop group based on the availability of epoll and kqueue.
     */
    public static EventLoopGroup createEventLoopGroup(int threads, String name) {
        if (KQueue.isAvailable()) {
            return new KQueueEventLoopGroup(threads, ThreadUtils.INSTANCE.newThreadFactory(name));
        }
        return new EpollEventLoopGroup(threads, ThreadUtils.INSTANCE.newThreadFactory(name));
    }

    public static void writeServerInfo(ServerInfoAccessor serverInfoAccessor, String socketPath, String infoFilePath) throws Exception {
        // cleanup socket path if it exists (unit test builder doesn't use one)
        if (socketPath != null) {
            Path path = Paths.get(socketPath);
            Files.deleteIfExists(path);
            if (Files.exists(path)) {
                log.error("Failed to clean up socket path {}. Exiting", socketPath);
            }
        }

        // write server info to file
        ServerInfo serverInfo = new ServerInfo(
                Protocol.UDS_PROTOCOL,
                Language.JAVA,
                serverInfoAccessor.getSDKVersion(),
                new HashMap<>());
        log.info("Writing server info {} to {}", serverInfo, infoFilePath);
        serverInfoAccessor.write(serverInfo, infoFilePath);
    }

    public static ServerBuilder<?> createServerBuilder(String socketPath, int maxMessageSize) {
        return NettyServerBuilder
                .forAddress(new DomainSocketAddress(socketPath))
                .channelType(GrpcServerUtils.getChannelTypeClass())
                .maxInboundMessageSize(maxMessageSize)
                .bossEventLoopGroup(GrpcServerUtils.createEventLoopGroup(1, "netty-boss"))
                .workerEventLoopGroup(GrpcServerUtils.createEventLoopGroup(ThreadUtils.INSTANCE.availableProcessors(), "netty-worker"));
    }
}
