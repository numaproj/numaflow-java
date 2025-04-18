package io.numaproj.numaflow.sourcetransformer;

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
 * Server is the gRPC server for executing source transformer operation.
 */
@Slf4j
public class Server {

    private final GRPCConfig grpcConfig;
    private final CompletableFuture<Void> shutdownSignal;
    private final ServerInfoAccessor serverInfoAccessor = new ServerInfoAccessorImpl(new ObjectMapper());
    private final GrpcServerWrapper server;

    /**
     * constructor to create gRPC server.
     *
     * @param sourceTransformer to process the message
     */
    public Server(SourceTransformer sourceTransformer) {
        this(sourceTransformer, GRPCConfig.defaultGrpcConfig());
    }

    /**
     * constructor to create gRPC server with gRPC config.
     *
     * @param grpcConfig to configure the max message size for grpc
     * @param sourceTransformer to transform the message
     */
    public Server(SourceTransformer sourceTransformer, GRPCConfig grpcConfig) {
        this.shutdownSignal = new CompletableFuture<>();
        this.grpcConfig = grpcConfig;
        this.server = new GrpcServerWrapper(this.grpcConfig, new Service(sourceTransformer, this.shutdownSignal));
    }

    @VisibleForTesting
    protected Server(GRPCConfig grpcConfig, SourceTransformer service, ServerInterceptor interceptor, String serverName) {
        this.shutdownSignal = new CompletableFuture<>();
        this.grpcConfig = grpcConfig;
        this.server = new GrpcServerWrapper(
                interceptor,
                serverName,
                new Service(service, this.shutdownSignal));
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
                    ContainerType.SOURCE_TRANSFORMER);

            // register shutdown hook to gracefully shut down the server
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    this.stop();
                    // FIXME - this is a workaround to immediately terminate the JVM process
                    // The correct way to do this is to stop all the actors and wait for them to terminate
                    System.exit(0);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    e.printStackTrace(System.err);
                }
            }));
        }

        this.server.start();

        log.info(
                "server started, listening on {}",
                this.grpcConfig.isLocal() ?
                        "localhost:" + this.grpcConfig.getPort() : this.grpcConfig.getSocketPath());

        // if there are any exceptions, shutdown the server gracefully.
        this.shutdownSignal.whenCompleteAsync((v, e) -> {
            if (e != null) {
                System.err.println("*** shutting down transformer gRPC server because of an exception - " + e.getMessage());
                try {
                    this.stop();
                    // FIXME - this is a workaround to immediately terminate the JVM process
                    // The correct way to do this is to stop all the actors and wait for them to terminate
                    System.exit(0);
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
        log.info("transformer server is waiting for termination");
        this.server.awaitTermination();
        log.info("transformer server has terminated");
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
