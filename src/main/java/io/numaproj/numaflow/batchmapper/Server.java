package io.numaproj.numaflow.batchmapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.ServerBuilder;
import io.numaproj.numaflow.info.ContainerType;
import io.numaproj.numaflow.info.ServerInfoAccessor;
import io.numaproj.numaflow.info.ServerInfoAccessorImpl;
import io.numaproj.numaflow.shared.GrpcServerHelper;
import io.numaproj.numaflow.shared.GrpcServerUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Server is the gRPC server for executing batch map operation.
 */
@Slf4j
public class Server {

    private final GRPCConfig grpcConfig;
    private final Service service;
    private final CompletableFuture<Void> shutdownSignal;
    private final ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessorImpl(new ObjectMapper());
    private io.grpc.Server server;
    private final GrpcServerHelper grpcServerHelper;

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
        this.shutdownSignal = new CompletableFuture<>();
        this.service = new Service(batchMapper, this.shutdownSignal);
        this.grpcConfig = grpcConfig;
        this.grpcServerHelper = new GrpcServerHelper();
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
            this.server = grpcServerHelper.createServer(
                    grpcConfig.getSocketPath(),
                    grpcConfig.getMaxMessageSize(),
                    grpcConfig.isLocal(),
                    grpcConfig.getPort(),
                    this.service);
        }

        server.start();

        log.info(
                "server started, listening on socket path: " + grpcConfig.getSocketPath());

        // register shutdown hook to gracefully shut down the server
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            if (server.isTerminated()) {
                return;
            }
            try {
                Server.this.stop();
                log.info("gracefully shutting down event loop groups");
                this.grpcServerHelper.gracefullyShutdownEventLoopGroups();
            } catch (InterruptedException e) {
                Thread.interrupted();
                e.printStackTrace(System.err);
            }
        }));

        // if there are any exceptions, shutdown the server gracefully.
        shutdownSignal.whenCompleteAsync((v, e) -> {
            if (server.isTerminated()) {
                return;
            }

            if (e != null) {
                System.err.println("*** shutting down batch map gRPC server because of an exception - " + e.getMessage());
                try {
                    log.info("stopping server");
                    Server.this.stop();
                    log.info("gracefully shutting down event loop groups");
                    this.grpcServerHelper.gracefullyShutdownEventLoopGroups();
                } catch (InterruptedException ex) {
                    Thread.interrupted();
                    ex.printStackTrace(System.err);
                }
            }
        });
    }

    /**
     * Blocks until the server has terminated. If the server is already terminated, this method
     * will return immediately. If the server is not yet terminated, this method will block the
     * calling thread until the server has terminated.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public void awaitTermination() throws InterruptedException {
        log.info("batch map server is waiting for termination");
        server.awaitTermination();
        log.info("batch map server has terminated");
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
