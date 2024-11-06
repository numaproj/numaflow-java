package io.numaproj.numaflow.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
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
 * Server is the gRPC server for executing map operation.
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
     * constructor to create gRPC server.
     *
     * @param mapper to process the message
     */
    public Server(Mapper mapper) {
        this(mapper, GRPCConfig.defaultGrpcConfig());
    }

    /**
     * constructor to create gRPC server with gRPC config.
     *
     * @param grpcConfig to configure the max message size for grpc
     * @param mapper to process the message
     */
    public Server(Mapper mapper, GRPCConfig grpcConfig) {
        this.shutdownSignal = new CompletableFuture<>();
        this.service = new Service(mapper, this.shutdownSignal);
        this.grpcConfig = grpcConfig;
        this.grpcServerHelper = new GrpcServerHelper();
    }

    /**
     * Starts the gRPC server and begins listening for requests. If the server is configured to be non-local,
     * it writes server information to a specified path. A shutdown hook is registered to ensure the server
     * is properly shut down when the JVM is shutting down.
     *
     * @throws Exception if the server fails to start
     */
    public void start() throws Exception {

        if (!grpcConfig.isLocal()) {
            GrpcServerUtils.writeServerInfo(
                    serverInfoAccessor,
                    grpcConfig.getSocketPath(),
                    grpcConfig.getInfoFilePath(),
                    ContainerType.MAPPER,
                    Collections.singletonMap(Constants.MAP_MODE_KEY, Constants.MAP_MODE));
        }

        if (this.server == null) {
            this.server = this.grpcServerHelper.createServer(
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

        // if there are any exceptions, shutdown the server gracefully.
        shutdownSignal.whenCompleteAsync((v, e) -> {
            if (server.isTerminated()) {
                return;
            }

            if (e != null) {
                System.err.println("*** shutting down mapper gRPC server because of an exception - " + e.getMessage());
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
        log.info("mapper server is waiting for termination");
        server.awaitTermination();
        log.info("mapper server has terminated");
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
            // force shutdown if not terminated
            if (!server.isTerminated()) {
                server.shutdownNow();
            }
        }
    }

    /**
     * Sets the server builder. This method can be used for testing purposes to provide a different
     * grpc server builder.
     *
     * @param serverBuilder the server builder to be used
     */
    public void setServerBuilder(ServerBuilder<?> serverBuilder) {
        this.server = serverBuilder
                .addService(this.service)
                .build();
    }
}
