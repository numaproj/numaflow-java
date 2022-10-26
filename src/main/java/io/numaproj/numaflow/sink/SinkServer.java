package io.numaproj.numaflow.sink;

import io.grpc.Server;
import io.grpc.ServerBuilder;
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

public class SinkServer {
  private static final Logger logger = Logger.getLogger(SinkServer.class.getName());

  private final String socketPath;
  private final ServerBuilder<?> serverBuilder;
  private Server server;
  private SinkHandler sinkHandler;

  public SinkServer() {
    this(Sink.SOCKET_PATH);
  }

  /**
   * GRPC server constructor
   *
   * @param socketPath A path that will be removed and used for unix domain socket (i.e. /var/run/numaflow/udsink.sock)
   */
  public SinkServer(String socketPath) {
    this(socketPath, new EpollEventLoopGroup());
  }

  public SinkServer(String socketPath, EpollEventLoopGroup group) {
    this(NettyServerBuilder
        .forAddress(new DomainSocketAddress(socketPath))
        .channelType(EpollServerDomainSocketChannel.class)
        .workerEventLoopGroup(group)
        .bossEventLoopGroup(group), socketPath);
  }

  public SinkServer(ServerBuilder<?> serverBuilder, String socketPath) {
    this.socketPath = socketPath;
    this.serverBuilder = serverBuilder;
  }

  public SinkServer registerSinker(SinkHandler sinkHandler) {
    this.sinkHandler = sinkHandler;
    return this;
  }

  /**
   * Start serving requests.
   */
  public void start() throws IOException {
    // cleanup socket path if it exists (unit test builder doesn't use one)
    if (socketPath != null) {
      Path path = Paths.get(socketPath);
      Files.deleteIfExists(path);
      if (Files.exists(path)) {
        logger.severe("Failed to clean up socket path \"" + socketPath + "\". Exiting");
      }
    }

    // build server
    server = serverBuilder
        .addService(new SinkService(sinkHandler))
        .build();

    // start server
    server.start();
    logger.info("Server started, listening on socket path: " + socketPath);

    // register shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      // Use stderr here since the logger may have been reset by its JVM shutdown hook.
      System.err.println("*** shutting down gRPC server since JVM is shutting down");
      try {
        SinkServer.this.stop();
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
