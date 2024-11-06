package io.numaproj.numaflow.shared;

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
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;

import static io.numaproj.numaflow.shared.GrpcServerUtils.DATUM_METADATA_WIN_END;
import static io.numaproj.numaflow.shared.GrpcServerUtils.DATUM_METADATA_WIN_START;
import static io.numaproj.numaflow.shared.GrpcServerUtils.WINDOW_END_TIME;
import static io.numaproj.numaflow.shared.GrpcServerUtils.WINDOW_START_TIME;

public class GrpcServerHelper {
    private EventLoopGroup bossEventLoopGroup;
    private EventLoopGroup workerEventLoopGroup;

    public void gracefullyShutdownEventLoopGroups() {
        if (this.bossEventLoopGroup != null) {
            this.bossEventLoopGroup.shutdownGracefully();
        }
        if (this.workerEventLoopGroup != null) {
            this.workerEventLoopGroup.shutdownGracefully();
        }
    }

    public Server createServer(
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
