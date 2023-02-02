package io.numaproj.numaflow.sink;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.numaproj.numaflow.common.GrpcServerConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class SinkServer {
    private static final Logger logger = Logger.getLogger(SinkServer.class.getName());

    private final GrpcServerConfig grpcServerConfig;
    private final ServerBuilder<?> serverBuilder;
    private final SinkService sinkService = new SinkService();
    private Server server;

    public SinkServer() {
        this(new GrpcServerConfig(Sink.SOCKET_PATH, Sink.DEFAULT_MESSAGE_SIZE));
    }

    /**
     * GRPC server constructor
     *
     * @param grpcServerConfig to configure the socket path and max message size for grpc
     */
    public SinkServer(GrpcServerConfig grpcServerConfig) {
        this(grpcServerConfig, new EpollEventLoopGroup());
    }

    public SinkServer(GrpcServerConfig grpcServerConfig, EpollEventLoopGroup group) {
        this(NettyServerBuilder
                .forAddress(new DomainSocketAddress(grpcServerConfig.getSocketPath()))
                .channelType(EpollServerDomainSocketChannel.class)
                .maxInboundMessageSize(grpcServerConfig.getMaxMessageSize())
                .workerEventLoopGroup(group)
                .bossEventLoopGroup(group), grpcServerConfig);
    }

    public SinkServer(ServerBuilder<?> serverBuilder, GrpcServerConfig grpcServerConfig) {
        this.grpcServerConfig = grpcServerConfig;
        this.serverBuilder = serverBuilder;
    }

    public SinkServer registerSinker(SinkHandler sinkHandler) {
        this.sinkService.setSinkHandler(sinkHandler);
        return this;
    }

    /**
     * Start serving requests.
     */
    public void start() throws IOException {
        // cleanup socket path if it exists (unit test builder doesn't use one)
        if (grpcServerConfig.getSocketPath()!=null) {
            Path path = Paths.get(grpcServerConfig.getSocketPath());
            Files.deleteIfExists(path);
            if (Files.exists(path)) {
                logger.severe("Failed to clean up socket path \"" + grpcServerConfig.getSocketPath()
                        + "\". Exiting");
            }
        }

        // build server
        server = serverBuilder
                .addService(this.sinkService)
                .build();

        // start server
        server.start();
        logger.info(
                "Server started, listening on socket path: " + grpcServerConfig.getSocketPath());

        // register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                SinkServer.this.stop();
            } catch (InterruptedException e) {
                Thread.interrupted();
                e.printStackTrace(System.err);
            }
            System.err.println("*** server shut down");
            this.sinkService.shutDown();
        }));
    }

    /**
     * Stop serving requests and shutdown resources. Await termination on the main thread since the
     * grpc library uses daemon threads.
     */
    public void stop() throws InterruptedException {
        if (server!=null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }
}
