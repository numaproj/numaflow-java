package io.numaproj.numaflow.batchmapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.ServerBuilder;
import io.numaproj.numaflow.info.ContainerType;
import io.numaproj.numaflow.info.ServerInfoAccessor;
import io.numaproj.numaflow.info.ServerInfoAccessorImpl;
import io.numaproj.numaflow.shared.GrpcServerUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Server is the gRPC server for executing batch map operation.
 */
@Slf4j
public class Server {

    private final GRPCConfig grpcConfig;
    private final Service service;
    private final ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessorImpl(new ObjectMapper());
    private io.grpc.Server server;

    /**
     * constructor to create sink gRPC server.
     *
     * @param batchMapper to process the message
     */
    public Server(BatchMapper batchMapper) {
        this(batchMapper, GRPCConfig.defaultGrpcConfig());
    }

    /**
     * constructor to create sink gRPC server with gRPC config.
     *
     * @param grpcConfig to configure the max message size for grpc
     * @param batchMapper to process the message
     */
    public Server(BatchMapper batchMapper, GRPCConfig grpcConfig) {
        this.service = new Service(batchMapper);
        this.grpcConfig = grpcConfig;
    }

    /**
     * Start serving requests.
     *
     * @throws Exception if server fails to start
     */
    public void start() throws Exception {
        GrpcServerUtils.writeServerInfo(
                serverInfoAccessor,
                grpcConfig.getSocketPath(),
                grpcConfig.getInfoFilePath(),
                ContainerType.MAPPER,
                Collections.singletonMap(Constants.MAP_MODE_KEY, Constants.MAP_MODE));

        if (this.server == null) {
            // create server builder
            ServerBuilder<?> serverBuilder = GrpcServerUtils.createServerBuilder(
                    grpcConfig.getSocketPath(),
                    grpcConfig.getMaxMessageSize(),
                    grpcConfig.isLocal(),
                    grpcConfig.getPort());
            // build server
            this.server = serverBuilder
                    .addService(this.service)
                    .build();
        }

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
     * Blocks until the server has terminated. If the server is already terminated, this method
     * will return immediately. If the server is not yet terminated, this method will block the
     * calling thread until the server has terminated.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    /**
     * Stop serving requests and shutdown resources. Await termination on the main thread since the
     * grpc library uses daemon threads.
     *
     * @throws InterruptedException if shutdown is interrupted
     */
    public void stop() throws InterruptedException {
        this.service.shutDown();
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            // force shutdown if not terminated
            if (!server.isTerminated()) {
                server.shutdownNow();
            }
        }
    }

    /**
     * Set server builder for testing.
     *
     * @param serverBuilder in process server builder can be used for testing
     */
    @VisibleForTesting
    public void setServerBuilder(ServerBuilder<?> serverBuilder) {
        this.server = serverBuilder
                .addService(this.service)
                .build();
    }
}
