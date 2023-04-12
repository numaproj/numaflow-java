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
import io.numaproj.numaflow.common.GRPCServerConfig;
import io.numaproj.numaflow.function.map.MapHandler;
import io.numaproj.numaflow.function.mapt.MapTHandler;
import io.numaproj.numaflow.function.reduce.Reducer;
import io.numaproj.numaflow.function.reduce.ReducerFactory;
import io.numaproj.numaflow.info.Language;
import io.numaproj.numaflow.info.Protocol;
import io.numaproj.numaflow.info.ServerInfo;
import io.numaproj.numaflow.info.ServerInfoAccessor;
import io.numaproj.numaflow.info.ServerInfoAccessorImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FunctionServer {

    private final GRPCServerConfig grpcServerConfig;
    private final ServerBuilder<?> serverBuilder;
    private final FunctionService functionService = new FunctionService();
    private final ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessorImpl(new ObjectMapper());
    private Server server;

    public FunctionServer() {
        this(new GRPCServerConfig());
    }

    /**
     * GRPC server constructor
     *
     * @param grpcServerConfig to configure the socket path and max message size for grpc
     */
    public FunctionServer(GRPCServerConfig grpcServerConfig) {
        this(grpcServerConfig, new EpollEventLoopGroup());
    }

    public FunctionServer(GRPCServerConfig grpcServerConfig, EpollEventLoopGroup group) {
        this(NettyServerBuilder
                .forAddress(new DomainSocketAddress(grpcServerConfig.getSocketPath()))
                .channelType(EpollServerDomainSocketChannel.class)
                .maxInboundMessageSize(grpcServerConfig.getMaxMessageSize())
                .workerEventLoopGroup(group)
                .bossEventLoopGroup(group), grpcServerConfig);
    }

    public FunctionServer(ServerBuilder<?> serverBuilder, GRPCServerConfig grpcServerConfig) {
        this.grpcServerConfig = grpcServerConfig;
        this.serverBuilder = serverBuilder;
    }

    public FunctionServer registerMapper(MapHandler mapHandler) {
        this.functionService.setMapHandler(mapHandler);
        return this;
    }

    public FunctionServer registerMapperT(MapTHandler mapTHandler) {
        this.functionService.setMapTHandler(mapTHandler);
        return this;
    }

    public FunctionServer registerReducerFactory(ReducerFactory<? extends Reducer> reducerFactory) {
        this.functionService.setReduceHandler(reducerFactory);
        return this;
    }

    /**
     * Start serving requests.
     */
    public void start() throws IOException {
        // cleanup socket path if it exists (unit test builder doesn't use one)
        if (grpcServerConfig.getSocketPath() != null) {
            Path path = Paths.get(grpcServerConfig.getSocketPath());
            Files.deleteIfExists(path);
            if (Files.exists(path)) {
                log.error("Failed to clean up socket path \"" + grpcServerConfig.getSocketPath()
                        + "\". Exiting");
            }
        }

        // write server info to file
        ServerInfo serverInfo = new ServerInfo(
                Protocol.UDS_PROTOCOL,
                Language.JAVA,
                serverInfoAccessor.getSDKVersion(),
                new HashMap<>());
        serverInfoAccessor.write(serverInfo, grpcServerConfig.getInfoFilePath());

        // build server
        ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {

                final var context =
                        Context.current().withValues(
                                FunctionConstants.DATUM_CONTEXT_KEY,
                                headers.get(FunctionConstants.DATUM_METADATA_KEY),
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
                "Server started, listening on socket path: " + grpcServerConfig.getSocketPath());

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
