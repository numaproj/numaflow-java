package io.numaproj.numaflow.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ServerBuilder;
import io.numaproj.numaflow.info.ServerInfoAccessor;
import io.numaproj.numaflow.info.ServerInfoAccessorImpl;
import io.numaproj.numaflow.shared.Constants;
import io.numaproj.numaflow.utils.GrpcServerUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

/**
 * MapServer is the gRPC server for executing map operation.
 */
@Slf4j
public class Server {

    private final GRPCConfig grpcConfig;
    private final Service service;
    private final ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessorImpl(new ObjectMapper());
    private io.grpc.Server server;

    /**
     * constructor to create gRPC server.
     * @param mapper to process the message
     */
    public Server(Mapper mapper) {
        this(mapper, new GRPCConfig(Constants.DEFAULT_MESSAGE_SIZE));
    }

    /**
     * constructor to create gRPC server with gRPC config.
     *
     * @param grpcConfig to configure the max message size for grpc
     * @param mapper to process the message
     */
    public Server(Mapper mapper, GRPCConfig grpcConfig) {
        this.service = new Service(mapper);
        this.grpcConfig = grpcConfig;
    }

    /**
     * Start serving requests.
     */
    public void start() throws Exception {
        GrpcServerUtils.writeServerInfo(serverInfoAccessor, grpcConfig.getSocketPath(), grpcConfig.getInfoFilePath());

        // create server builder
        ServerBuilder<?> serverBuilder = GrpcServerUtils.createServerBuilder(
                grpcConfig.getSocketPath(), grpcConfig.getMaxMessageSize());

        // build server
        this.server = serverBuilder
                .addService(this.service)
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
                Server.this.stop();
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
