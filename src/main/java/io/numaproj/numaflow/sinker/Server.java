package io.numaproj.numaflow.sinker;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ServerBuilder;
import io.numaproj.numaflow.info.ContainerType;
import io.numaproj.numaflow.info.ServerInfoAccessor;
import io.numaproj.numaflow.info.ServerInfoAccessorImpl;
import io.numaproj.numaflow.shared.GrpcServerHelper;
import io.numaproj.numaflow.shared.GrpcServerUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Server is the gRPC server for executing user defined sinks.
 */
@Slf4j
public class Server {

    private final GRPCConfig grpcConfig;
    private final Service service;
    public final CompletableFuture<Void> shutdownSignal;
    private final ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessorImpl(new ObjectMapper());
    private io.grpc.Server server;
    private final GrpcServerHelper grpcServerHelper;

    /**
     * constructor to create sink gRPC server.
     *
     * @param sinker sink to process the message
     */
    public Server(Sinker sinker) {
        this(sinker, GRPCConfig.defaultGrpcConfig());
    }

    /**
     * constructor to create sink gRPC server with gRPC config.
     *
     * @param grpcConfig to configure the max message size for grpc
     * @param sinker sink to process the message
     */
    public Server(Sinker sinker, GRPCConfig grpcConfig) {
        this.shutdownSignal = new CompletableFuture<>();
        this.service = new Service(sinker, this.shutdownSignal);
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
                    ContainerType.SINKER);
        }

        if (this.server == null) {
            // create server builder
            ServerBuilder<?> serverBuilder = this.grpcServerHelper.createServerBuilder(
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
                "server started, listening on {}",
                grpcConfig.isLocal() ?
                        "localhost:" + grpcConfig.getPort() : grpcConfig.getSocketPath());

        // register shutdown hook to gracefully shut down the server
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down sink gRPC server since JVM is shutting down");
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
                System.err.println("*** shutting down sink gRPC server because of an exception - " + e.getMessage());
                try {
                    log.info("stopping server");
                    Server.this.stop();
                    log.info("gracefully shutdown event loop groups");
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
        log.info("sink server is waiting for termination");
        server.awaitTermination();
        log.info("sink server is terminated");
    }

    /**
     * Stop serving requests and shutdown resources. Await termination on the main thread since the
     * grpc library uses daemon threads.
     *
     * @throws InterruptedException if shutdown is interrupted
     */
    public void stop() throws InterruptedException {
        log.info("server.stop started. Shutting down sink service");
        this.service.shutDown();
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            // force shutdown if not terminated
            if (!server.isTerminated()) {
                log.info("server did not terminate in {} seconds. Shutting down forcefully", 30);
                server.shutdownNow();
            }
        }
        log.info("server.stop successfully completed");
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
