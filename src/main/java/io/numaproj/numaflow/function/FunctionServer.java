package io.numaproj.numaflow.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.numaproj.numaflow.function.map.MapHandler;
import io.numaproj.numaflow.function.mapt.MapTHandler;
import io.numaproj.numaflow.function.reduce.ReduceHandler;
import io.numaproj.numaflow.function.reduce.ReducerFactory;
import io.numaproj.numaflow.info.Language;
import io.numaproj.numaflow.info.Protocol;
import io.numaproj.numaflow.info.ServerInfo;
import io.numaproj.numaflow.info.ServerInfoAccessor;
import io.numaproj.numaflow.info.ServerInfoAccessorImpl;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FunctionServer {

    private final FunctionGRPCConfig grpcConfig;
    private final ServerBuilder<?> serverBuilder;
    private final FunctionService functionService = new FunctionService();
    private final ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessorImpl(new ObjectMapper());
    private Server server;

    public FunctionServer() {
        this(new FunctionGRPCConfig(FunctionConstants.DEFAULT_MESSAGE_SIZE));
    }

    /**
     * GRPC server constructor
     *
     * @param FunctionGRPCConfig to configure max message size for grpc
     */
    public FunctionServer(FunctionGRPCConfig grpcConfig) {
        this(grpcConfig, new EpollEventLoopGroup());
    }

    public FunctionServer(FunctionGRPCConfig grpcConfig, EpollEventLoopGroup group) {
        this(NettyServerBuilder
                .forAddress(new DomainSocketAddress(grpcConfig.getSocketPath()))
                .channelType(EpollServerDomainSocketChannel.class)
                .maxInboundMessageSize(grpcConfig.getMaxMessageSize())
                .workerEventLoopGroup(group)
                .bossEventLoopGroup(group), grpcConfig);
    }

    public FunctionServer(ServerBuilder<?> serverBuilder, FunctionGRPCConfig grpcConfig) {
        this.grpcConfig = grpcConfig;
        this.serverBuilder = serverBuilder;
    }

    public FunctionServer registerMapHandler(MapHandler mapHandler) {
        this.functionService.setMapHandler(mapHandler);
        return this;
    }

    public FunctionServer registerMapTHandler(MapTHandler mapTHandler) {
        this.functionService.setMapTHandler(mapTHandler);
        return this;
    }

    public FunctionServer registerReducerFactory(ReducerFactory<? extends ReduceHandler> reducerFactory) {
        this.functionService.setReduceHandler(reducerFactory);
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
        serverInfoAccessor.write(serverInfo, infoFilePath);

        // build server
        ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {

                final var context =
                        Context.current().withValues(
                                FunctionConstants.WINDOW_START_TIME,
                                headers.get(FunctionConstants.DATUM_METADATA_WIN_START),
                                FunctionConstants.WINDOW_END_TIME,
                                headers.get(FunctionConstants.DATUM_METADATA_WIN_END));
                return Contexts.interceptCall(context, call, headers, next);
            }
        };
        server = serverBuilder
                .addService(functionService)
                .intercept(interceptor)
                .build();

        // start server
        server.start();
        log.info(
                "Server started, listening on socket path: " + grpcConfig.getSocketPath());

        // register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            FunctionServer.this.stop();
            System.err.println("*** server shut down");
        }));
    }

    /**
     * Stop serving requests and shutdown resources. Await termination on the main thread since the
     * grpc library uses daemon threads.
     */
    public void stop() {
        if (server != null) {
            try {
                server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.interrupted();
                e.printStackTrace(System.err);
            }
        }
    }
}
