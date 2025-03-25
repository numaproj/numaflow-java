package io.numaproj.numaflow.sinker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.ServerInterceptor;
import io.numaproj.numaflow.info.ContainerType;
import io.numaproj.numaflow.info.ServerInfoAccessor;
import io.numaproj.numaflow.info.ServerInfoAccessorImpl;
import io.numaproj.numaflow.shared.GrpcServerUtils;
import io.numaproj.numaflow.shared.GrpcServerWrapper;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

/**
 * Server is the gRPC server for executing user defined sinks.
 */
@Slf4j
public class Server {

    private final GRPCConfig grpcConfig;
    private final CompletableFuture<Void> shutdownSignal;
    private final ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessorImpl(new ObjectMapper());
    private final Service service;
    private final GrpcServerWrapper server;

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
        this.grpcConfig = grpcConfig;
        this.service = new Service(sinker, this.shutdownSignal);
        this.server = new GrpcServerWrapper(grpcConfig, this.service);
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

            // register shutdown hook to gracefully shut down the server
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down sink gRPC server since JVM is shutting down");
                try {
                    this.stop();
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    e.printStackTrace(System.err);
                }
            }));
        }

        server.start();

        log.info(
                "server started, listening on {}",
                grpcConfig.isLocal() ?
                        "localhost:" + grpcConfig.getPort() : grpcConfig.getSocketPath());

        // if there are any exceptions, shutdown the server gracefully.
        shutdownSignal.whenCompleteAsync((v, e) -> {
            if (e != null) {
                System.err.println("*** shutting down sink gRPC server because of an exception - "
                        + e.getMessage());
                try {
                    this.stop();
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
        server.gracefullyShutdown();
        this.service.shutDown();
    }

    @VisibleForTesting
    protected Server(
            GRPCConfig grpcConfig,
            Sinker sinker,
            ServerInterceptor interceptor,
            String serverName) {
        this.grpcConfig = grpcConfig;
        this.shutdownSignal = new CompletableFuture<>();
        this.service = new Service(sinker, this.shutdownSignal);
        this.server = new GrpcServerWrapper(interceptor, serverName, this.service);
    }
}
