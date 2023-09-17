package io.numaproj.numaflow.sideinput;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.ServerBuilder;
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
    private io.grpc.Server server;

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
    }

    /**
     * Start serving requests.
     *
     * @throws Exception if server fails to start
     */
    public void start() throws Exception {

        if (this.server == null) {
            // create server builder
            ServerBuilder<?> serverBuilder = GrpcServerUtils.createServerBuilder(
                    grpcConfig.getSocketPath(), grpcConfig.getMaxMessageSize());

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
     * Stop serving requests and shutdown resources. Await termination on the main thread since the
     * grpc library uses daemon threads.
     *
     * @throws InterruptedException if shutdown is interrupted
     */
    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Set server builder for testing.
     *
     * @param serverBuilder
     */
    @VisibleForTesting
    public void setServerBuilder(ServerBuilder<?> serverBuilder) {
        this.server = serverBuilder
                .addService(this.service)
                .build();
    }
}
