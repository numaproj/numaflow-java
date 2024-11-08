package io.numaproj.numaflow.sideinput;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.ServerInterceptor;
import io.numaproj.numaflow.info.ContainerType;
import io.numaproj.numaflow.info.ServerInfoAccessor;
import io.numaproj.numaflow.info.ServerInfoAccessorImpl;
import io.numaproj.numaflow.shared.GrpcServerUtils;
import io.numaproj.numaflow.shared.GrpcServerWrapper;
import lombok.extern.slf4j.Slf4j;

/**
 * Server is the gRPC server for retrieving side input.
 */
@Slf4j
public class Server {

    private final GRPCConfig grpcConfig;
    private final ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessorImpl(new ObjectMapper());
    private final GrpcServerWrapper server;

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
        this.grpcConfig = grpcConfig;
        this.server = new GrpcServerWrapper(this.grpcConfig, new Service(sideInputRetriever));
    }

    @VisibleForTesting
    protected Server(GRPCConfig grpcConfig, SideInputRetriever service, ServerInterceptor interceptor, String serverName) {
        this.grpcConfig = grpcConfig;
        this.server = new GrpcServerWrapper(
                interceptor,
                serverName,
                new Service(service));
    }

    /**
     * Start serving requests.
     *
     * @throws Exception if server fails to start
     */
    public void start() throws Exception {
        if (!this.grpcConfig.isLocal()) {
            GrpcServerUtils.writeServerInfo(
                    this.serverInfoAccessor,
                    this.grpcConfig.getSocketPath(),
                    this.grpcConfig.getInfoFilePath(),
                    ContainerType.SIDEINPUT);
        }

        this.server.start();

        log.info(
                "server started, listening on {}",
                this.grpcConfig.isLocal() ?
                        "localhost:" + this.grpcConfig.getPort() : this.grpcConfig.getSocketPath());

        // register shutdown hook to gracefully shut down the server
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            try {
                this.stop();
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
        this.server.awaitTermination();
        log.info("side input server has terminated");
    }

    /**
     * Stop serving requests and shutdown resources. Await termination on the main thread since the
     * grpc library uses daemon threads.
     *
     * @throws InterruptedException if shutdown is interrupted
     */
    public void stop() throws InterruptedException {
        this.server.gracefullyShutdown();
    }
}
