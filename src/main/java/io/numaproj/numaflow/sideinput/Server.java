package io.numaproj.numaflow.sideinput;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.ServerBuilder;
import io.numaproj.numaflow.info.ContainerType;
import io.numaproj.numaflow.info.ServerInfoAccessor;
import io.numaproj.numaflow.info.ServerInfoAccessorImpl;
import io.numaproj.numaflow.shared.GrpcServerHelper;
import io.numaproj.numaflow.shared.GrpcServerUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

/**
 * Server is the gRPC server for retrieving side input.
 */
@Slf4j
public class Server {

    private final GRPCConfig grpcConfig;
    private final Service service;
    private final ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessorImpl(new ObjectMapper());
    private io.grpc.Server server;
    private final GrpcServerHelper grpcServerHelper;

    /**
     * constructor to create gRPC server.
     *
     * @param sideInputRetriever to retrieve the side input
     */
    public Server(SideInputRetriever sideInputRetriever) {
        this(sideInputRetriever, GRPCConfig.defaultGrpcConfig());
    }

    /**
     * constructor to create gRPC server with gRPC config.
     *
     * @param grpcConfig to configure the max message size for grpc
     * @param sideInputRetriever to retrieve the side input
     */
    public Server(SideInputRetriever sideInputRetriever, GRPCConfig grpcConfig) {
        this.service = new Service(sideInputRetriever);
        this.grpcConfig = grpcConfig;
        this.grpcServerHelper = new GrpcServerHelper();
    }

    /**
     * Start serving requests.
     *
     * @throws Exception if server fails to start
     */
    public void start() throws Exception {
        if (!grpcConfig.isLocal()) {
            GrpcServerUtils.writeServerInfo(
                    serverInfoAccessor,
                    grpcConfig.getSocketPath(),
                    grpcConfig.getInfoFilePath(),
                    ContainerType.SIDEINPUT);
        }

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
                "server started, listening on {}",
                grpcConfig.isLocal() ?
                        "localhost:" + grpcConfig.getPort() : grpcConfig.getSocketPath());

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
    }

    /**
     * Blocks until the server has terminated. If the server is already terminated, this method
     * will return immediately. If the server is not yet terminated, this method will block the
     * calling thread until the server has terminated.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public void awaitTermination() throws InterruptedException {
        log.info("side input server is waiting for termination");
        server.awaitTermination();
        log.info("side input server has terminated");
    }

    /**
     * Stop serving requests and shutdown resources. Await termination on the main thread since the
     * grpc library uses daemon threads.
     *
     * @throws InterruptedException if shutdown is interrupted
     */
    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
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
