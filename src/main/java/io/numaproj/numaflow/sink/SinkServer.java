package io.numaproj.numaflow.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.unix.DomainSocketAddress;
import io.numaproj.numaflow.info.Language;
import io.numaproj.numaflow.info.Protocol;
import io.numaproj.numaflow.info.ServerInfo;
import io.numaproj.numaflow.info.ServerInfoAccessor;
import io.numaproj.numaflow.info.ServerInfoAccessorImpl;
import io.numaproj.numaflow.sink.handler.SinkHandler;
import io.numaproj.numaflow.utils.GrpcServerUtils;
import io.numaproj.numaflow.utils.ThreadUtils;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * SinkServer is the gRPC server for executing user defined sinks.
 */
@Slf4j
public class SinkServer {

    private final SinkGRPCConfig grpcConfig;
    private final ServerBuilder<?> serverBuilder;
    private final SinkService sinkService = new SinkService();
    private final ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessorImpl(new ObjectMapper());
    private Server server;

    /**
     * constructor to create sink gRPC server.
     */
    public SinkServer() {
        this(new SinkGRPCConfig(SinkConstants.DEFAULT_MESSAGE_SIZE));
    }

    /**
     * constructor to create sink gRPC server with gRPC config.
     *
     * @param grpcConfig to configure the max message size for grpc
     */
    public SinkServer(SinkGRPCConfig grpcConfig) {
        this(NettyServerBuilder
                .forAddress(new DomainSocketAddress(grpcConfig.getSocketPath()))
                .channelType(GrpcServerUtils.getChannelTypeClass())
                .maxInboundMessageSize(grpcConfig.getMaxMessageSize())
                .bossEventLoopGroup(GrpcServerUtils.createEventLoopGroup(1, "netty-boss"))
                .workerEventLoopGroup(GrpcServerUtils.createEventLoopGroup(ThreadUtils.INSTANCE.availableProcessors(), "netty-worker"))
                , grpcConfig);
    }

    public SinkServer(ServerBuilder<?> serverBuilder, SinkGRPCConfig grpcConfig) {
        this.grpcConfig = grpcConfig;
        this.serverBuilder = serverBuilder;
    }

    /**
     * registers the sink handler to the gRPC server.
     * @param sinkHandler handler to process the message
     * @return returns the server.
     */
    public SinkServer registerSinker(SinkHandler sinkHandler) {
        this.sinkService.setSinkHandler(sinkHandler);
        return this;
    }

    /**
     * Start serving requests.
     */
    public void start() throws Exception {
        String socketPath = grpcConfig.getSocketPath();
        String infoFilePath = grpcConfig.getInfoFilePath();
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

        // build server
        server = serverBuilder
                .addService(this.sinkService)
                .build();

        // start server
        server.start();
        log.info(
                "Server started, listening on socket path: " + grpcConfig.getSocketPath());

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
        }));
    }

    /**
     * Stop serving requests and shutdown resources. Await termination on the main thread since the
     * grpc library uses daemon threads.
     */
    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }
}
