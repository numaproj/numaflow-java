package io.numaproj.numaflow.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.unix.DomainSocketAddress;
import io.numaproj.numaflow.function.handlers.MapHandler;
import io.numaproj.numaflow.function.handlers.MapStreamHandler;
import io.numaproj.numaflow.function.handlers.MapTHandler;
import io.numaproj.numaflow.function.handlers.ReduceHandler;
import io.numaproj.numaflow.function.handlers.ReducerFactory;
import io.numaproj.numaflow.info.Language;
import io.numaproj.numaflow.info.Protocol;
import io.numaproj.numaflow.info.ServerInfo;
import io.numaproj.numaflow.info.ServerInfoAccessor;
import io.numaproj.numaflow.info.ServerInfoAccessorImpl;
import io.numaproj.numaflow.utils.GrpcServerUtils;
import io.numaproj.numaflow.utils.ThreadUtils;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * FunctionServer is the gRPC server for executing user defined functions.
 */
@Slf4j
public class FunctionServer {

    private final FunctionGRPCConfig grpcConfig;
    private final ServerBuilder<?> serverBuilder;
    private final FunctionService functionService = new FunctionService();
    private final ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessorImpl(new ObjectMapper());
    private Server server;

    /**
     * constructor to create function gRPC server.
     */
    public FunctionServer() {
        this(new FunctionGRPCConfig(FunctionConstants.DEFAULT_MESSAGE_SIZE));
    }

    /**
     * constructor to create function gRPC server with gRPC config.
     *
     */
    public FunctionServer(FunctionGRPCConfig grpcConfig) {
        this(NettyServerBuilder
                .forAddress(new DomainSocketAddress(grpcConfig.getSocketPath()))
                .channelType(GrpcServerUtils.getChannelTypeClass())
                .maxInboundMessageSize(grpcConfig.getMaxMessageSize())
                .bossEventLoopGroup(GrpcServerUtils.createEventLoopGroup(1, "netty-boss"))
                .workerEventLoopGroup(GrpcServerUtils.createEventLoopGroup(ThreadUtils.INSTANCE.availableProcessors(), "netty-worker"))
                , grpcConfig);
    }

    @VisibleForTesting
    public FunctionServer(ServerBuilder<?> serverBuilder, FunctionGRPCConfig grpcConfig) {
        this.grpcConfig = grpcConfig;
        this.serverBuilder = serverBuilder;
    }

    /**
     * registers the map handler to the server.
     * @param mapHandler handler to process the message.
     * @return returns a new Function gRPC server.
     */
    public FunctionServer registerMapHandler(MapHandler mapHandler) {
        this.functionService.setMapHandler(mapHandler);
        return this;
    }

    /**
     * registers the mapT handler to the server.
     * @param mapTHandler handler to process the message and assign event time.
     * @return returns a new Function gRPC server.
     */
    public FunctionServer registerMapTHandler(MapTHandler mapTHandler) {
        this.functionService.setMapTHandler(mapTHandler);
        return this;
    }

    /**
     * registers the map stream handler to the server.
     * @param mapStreamHandler handler to process the message and assign event time.
     * @return returns a new Function gRPC server.
     */
    public FunctionServer registerMapStreamHandler(MapStreamHandler mapStreamHandler) {
        this.functionService.setMapStreamHandler(mapStreamHandler);
        return this;
    }

    /**
     * registers the reducer factory to the server.
     * @param reducerFactory which produces reduce handlers to perform reduce operation.
     * @return returns a new Function gRPC server.
     */
    public FunctionServer registerReducerFactory(ReducerFactory<? extends ReduceHandler> reducerFactory) {
        this.functionService.setReduceHandler(reducerFactory);
        return this;
    }


    /**
     * Starts the function server.
     * @throws Exception if there are any exceptions while processing the request.
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
                ServerCall.Listener<ReqT> listener = Contexts.interceptCall(context, call, headers, next);
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
                    @Override
                    public void onHalfClose() {
                        try {
                            super.onHalfClose();
                        } catch (RuntimeException ex) {
                            handleException(ex, call, headers);
                            throw ex;
                        }
                    }
                    private void handleException(RuntimeException e, ServerCall<ReqT, RespT> serverCall, Metadata headers) {
                        // Currently, we only have application level exceptions.
                        // Translate it to UNKNOWN status.
                        var status = Status.UNKNOWN.withDescription(e.getMessage()).withCause(e);
                        var newStatus = Status.fromThrowable(status.asException());
                        serverCall.close(newStatus, headers);
                    }
                };
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
