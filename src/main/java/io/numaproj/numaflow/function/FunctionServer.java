package io.numaproj.numaflow.function;

import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class FunctionServer {
  private static final Logger logger = Logger.getLogger(FunctionServer.class.getName());

  private final GrpcServerConfig grpcServerConfig;
  private final ServerBuilder<?> serverBuilder;
  private Server server;
  private MapHandler mapHandler;

  public FunctionServer() {
    this(new GrpcServerConfig());
  }

  /**
   * GRPC server constructor
   *
   * @param grpcServerConfig to configure the socket path and max message size for grpc
   */
  public FunctionServer(GrpcServerConfig grpcServerConfig) {
    this(grpcServerConfig, new EpollEventLoopGroup());
  }

  public FunctionServer(GrpcServerConfig grpcServerConfig, EpollEventLoopGroup group) {
    this(NettyServerBuilder
            .forAddress(new DomainSocketAddress(grpcServerConfig.getSocketPath()))
            .channelType(EpollServerDomainSocketChannel.class)
            .maxInboundMessageSize(grpcServerConfig.getMaxMessageSize())
            .workerEventLoopGroup(group)
            .bossEventLoopGroup(group), grpcServerConfig);
  }

  public FunctionServer(ServerBuilder<?> serverBuilder, GrpcServerConfig grpcServerConfig) {
    this.grpcServerConfig = grpcServerConfig;
    this.serverBuilder = serverBuilder;
  }

  public FunctionServer registerMapper(MapHandler mapHandler) {
    this.mapHandler = mapHandler;
    return this;
  }

  /**
   * Start serving requests.
   */
  public void start() throws IOException {
    // cleanup socket path if it exists (unit test builder doesn't use one)
    if (grpcServerConfig.getSocketPath() != null) {
      Path path = Paths.get(grpcServerConfig.getSocketPath());
      Files.deleteIfExists(path);
      if (Files.exists(path)) {
        logger.severe("Failed to clean up socket path \"" + grpcServerConfig.getSocketPath() + "\". Exiting");
      }
    }

    // build server
    ServerInterceptor interceptor = new ServerInterceptor() {
      @Override
      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
          ServerCall<ReqT, RespT> call,
          Metadata headers,
          ServerCallHandler<ReqT, RespT> next) {

        final var context =
            Context.current().withValue(Function.DATUM_CONTEXT_KEY, headers.get(Function.DATUM_METADATA_KEY));
        return Contexts.interceptCall(context, call, headers, next);
      }
    };
    server = serverBuilder
        .addService(new FunctionService(mapHandler))
        .intercept(interceptor)
        .build();

    // start server
    server.start();
    logger.info("Server started, listening on socket path: " + grpcServerConfig.getSocketPath());

    // register shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      // Use stderr here since the logger may have been reset by its JVM shutdown hook.
      System.err.println("*** shutting down gRPC server since JVM is shutting down");
      try {
        FunctionServer.this.stop();
      } catch (InterruptedException e) {
        e.printStackTrace(System.err);
      }
      System.err.println("*** server shut down");
    }));
  }

  /**
   * Stop serving requests and shutdown resources.
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  public void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }
}
