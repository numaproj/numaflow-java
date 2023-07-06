package io.numaproj.numaflow.function.utils;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerDomainSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * GrpcServerUtils is the utility class for netty server channel.
 */
public class GrpcServerUtils {
    /*
        * Get the server socket channel class based on the availability of epoll and kqueue.
     */
    public static Class<? extends ServerChannel> getChannelTypeClass() {
        if (Epoll.isAvailable()) {
            return EpollServerDomainSocketChannel.class;
        }
        if (KQueue.isAvailable()) {
            return KQueueServerDomainSocketChannel.class;
        }
        return NioServerSocketChannel.class;
    }

    /*
        * Get the event loop group based on the availability of epoll and kqueue.
     */
    public static EventLoopGroup createEventLoopGroup(int threads, String name) {
        if (Epoll.isAvailable()) {
            return new EpollEventLoopGroup(threads, ThreadUtils.INSTANCE.newThreadFactory(name));
        }
        if (KQueue.isAvailable()) {
            return new KQueueEventLoopGroup(threads, ThreadUtils.INSTANCE.newThreadFactory(name));
        }
        return new NioEventLoopGroup(threads, ThreadUtils.INSTANCE.newThreadFactory(name));
    }
}
