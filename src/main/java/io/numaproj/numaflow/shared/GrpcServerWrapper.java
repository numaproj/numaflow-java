package io.numaproj.numaflow.shared;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.BindableService;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

import static io.numaproj.numaflow.shared.GrpcServerUtils.DATUM_METADATA_WIN_END;
import static io.numaproj.numaflow.shared.GrpcServerUtils.DATUM_METADATA_WIN_START;
import static io.numaproj.numaflow.shared.GrpcServerUtils.WINDOW_END_TIME;
import static io.numaproj.numaflow.shared.GrpcServerUtils.WINDOW_START_TIME;

/**
 * GrpcServerWrapper is a wrapper class for gRPC server.
 * It takes care of creating, starting and gracefully shutting down the server.
 */
@Slf4j
public class GrpcServerWrapper {
    private final Server server;
    private EventLoopGroup bossEventLoopGroup;
    private EventLoopGroup workerEventLoopGroup;

    public GrpcServerWrapper(
            GrpcConfigRetriever grpcConfigRetriever,
            BindableService service) {
        this.server = createServer(
                grpcConfigRetriever.getSocketPath(),
                grpcConfigRetriever.getMaxMessageSize(),
                grpcConfigRetriever.isLocal(),
                grpcConfigRetriever.getPort(),
                service);
    }

    @VisibleForTesting
    public GrpcServerWrapper(
            ServerInterceptor interceptor,
            String serverName,
            BindableService service) {
        if (interceptor == null) {
            this.server = InProcessServerBuilder.forName(serverName)
                    .directExecutor()
                    .addService(service)
                    .build();
            return;
        }
        this.server = InProcessServerBuilder.forName(serverName)
                .intercept(interceptor)
                .directExecutor()
                .addService(service)
                .build();
    }

    public void start() throws Exception {
        if (this.server == null) {
            throw new IllegalStateException("Server is not initialized");
        }
        this.server.start();
    }

    public void awaitTermination() throws InterruptedException {
        this.server.awaitTermination();
        // if the server has been terminated, we should expect the event loop groups to be terminated as well.
        if (!(this.workerEventLoopGroup.awaitTermination(30, TimeUnit.SECONDS) &&
                this.bossEventLoopGroup.awaitTermination(30, TimeUnit.SECONDS))) {
            log.error("Timed out to gracefully shutdown event loop groups");
            throw new InterruptedException("Timed out to gracefully shutdown event loop groups");
        }
    }

    public void gracefullyShutdown() throws InterruptedException{
        if (this.server == null || this.server.isTerminated()) {
            return;
        }
        log.info("stopping gRPC server...");
        this.server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        if (!this.server.isTerminated()) {
            this.server.shutdownNow();
        }
        log.info("gracefully shutting down event loop groups...");
        this.gracefullyShutdownEventLoopGroups();
    }

    private void gracefullyShutdownEventLoopGroups() {
        if (this.bossEventLoopGroup != null) {
            this.bossEventLoopGroup.shutdownGracefully();
        }
        if (this.workerEventLoopGroup != null) {
            this.workerEventLoopGroup.shutdownGracefully();
        }
    }

    private Server createServer(
            String socketPath,
            int maxMessageSize,
            boolean isLocal,
            int port,
            BindableService service) {
        ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    io.grpc.Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {

                final var context =
                        Context.current().withValues(
                                WINDOW_START_TIME,
                                headers.get(DATUM_METADATA_WIN_START),
                                WINDOW_END_TIME,
                                headers.get(DATUM_METADATA_WIN_END));
                ServerCall.Listener<ReqT> listener = Contexts.interceptCall(
                        context,
                        call,
                        headers,
                        next);
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(
                        listener) {
                    @Override
                    public void onHalfClose() {
                        try {
                            super.onHalfClose();
                        } catch (RuntimeException ex) {
                            handleException(ex, call, headers);
                            throw ex;
                        }
                    }

                    private void handleException(
                            RuntimeException e,
                            ServerCall<ReqT, RespT> serverCall,
                            io.grpc.Metadata headers) {
                        // Currently, we only have application level exceptions.
                        // Translate it to UNKNOWN status.
                        var status = Status.UNKNOWN.withDescription(e.getMessage()).withCause(e);
                        var newStatus = Status.fromThrowable(status.asException());
                        serverCall.close(newStatus, headers);
                        e.printStackTrace();
                        System.exit(1);
                    }
                };
            }
        };

        if (isLocal) {
            return ServerBuilder.forPort(port)
                    .maxInboundMessageSize(maxMessageSize)
                    .intercept(interceptor)
                    .addService(service)
                    .build();
        }

        this.bossEventLoopGroup = GrpcServerUtils.createEventLoopGroup(1, "netty-boss");
        this.workerEventLoopGroup = GrpcServerUtils.createEventLoopGroup(
                ThreadUtils.INSTANCE.availableProcessors(),
                "netty-worker");

        return NettyServerBuilder
                .forAddress(new DomainSocketAddress(socketPath))
                .channelType(GrpcServerUtils.getChannelTypeClass())
                .maxInboundMessageSize(maxMessageSize)
                .bossEventLoopGroup(this.bossEventLoopGroup)
                .workerEventLoopGroup(this.workerEventLoopGroup)
                .intercept(interceptor)
                .addService(service)
                .build();
    }
}
