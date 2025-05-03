package io.numaproj.numaflow.shared;

import static io.numaproj.numaflow.info.ServerInfo.MINIMUM_NUMAFLOW_VERSION;

import io.grpc.Context;
import io.grpc.Metadata;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerDomainSocketChannel;
import io.numaproj.numaflow.info.ContainerType;
import io.numaproj.numaflow.info.Language;
import io.numaproj.numaflow.info.Protocol;
import io.numaproj.numaflow.info.ServerInfo;
import io.numaproj.numaflow.info.ServerInfoAccessor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** GrpcServerUtils is the utility class for netty server channel. */
@Slf4j
public class GrpcServerUtils {

  public static final String WIN_START_KEY = "x-numaflow-win-start-time";
  public static final String WIN_END_KEY = "x-numaflow-win-end-time";
  public static final Context.Key<String> WINDOW_START_TIME =
      Context.keyWithDefault(WIN_START_KEY, "");
  public static final Context.Key<String> WINDOW_END_TIME = Context.keyWithDefault(WIN_END_KEY, "");
  public static final Metadata.Key<String> DATUM_METADATA_WIN_START =
      Metadata.Key.of(WIN_START_KEY, Metadata.ASCII_STRING_MARSHALLER);
  public static final Metadata.Key<String> DATUM_METADATA_WIN_END =
      Metadata.Key.of(WIN_END_KEY, Metadata.ASCII_STRING_MARSHALLER);

  // private constructor to prevent instantiation
  private GrpcServerUtils() {
    throw new IllegalArgumentException(
        "Utility class 'GrpcServerUtils' should not be instantiated");
  }

  /*
   * Get the server socket channel class based on the availability of epoll and kqueue.
   */
  public static Class<? extends ServerChannel> getChannelTypeClass() {
    if (KQueue.isAvailable()) {
      return KQueueServerDomainSocketChannel.class;
    }
    return EpollServerDomainSocketChannel.class;
  }

  /*
   * Get the event loop group based on the availability of epoll and kqueue.
   */
  public static EventLoopGroup createEventLoopGroup(int threads, String name) {
    if (KQueue.isAvailable()) {
      return new KQueueEventLoopGroup(threads, ThreadUtils.INSTANCE.newThreadFactory(name));
    }
    return new EpollEventLoopGroup(threads, ThreadUtils.INSTANCE.newThreadFactory(name));
  }

  public static void writeServerInfo(
      ServerInfoAccessor serverInfoAccessor,
      String socketPath,
      String infoFilePath,
      ContainerType containerType)
      throws Exception {
    writeServerInfo(serverInfoAccessor, socketPath, infoFilePath, containerType, new HashMap<>());
  }

  public static void writeServerInfo(
      ServerInfoAccessor serverInfoAccessor,
      String socketPath,
      String infoFilePath,
      ContainerType containerType,
      Map<String, String> metaData)
      throws Exception {
    // cleanup socket path if it exists (unit test builder doesn't use one)
    if (socketPath != null) {
      Path path = Paths.get(socketPath);
      Files.deleteIfExists(path);
      if (Files.exists(path)) {
        log.error("Failed to clean up socket path {}. Exiting", socketPath);
      }
    }

    // server info file can be null if the Grpc server is used for local component testing
    // write server info to file if file path is not null
    if (infoFilePath == null) {
      return;
    }

    if (metaData == null) {
      metaData = new HashMap<>();
    }

    ServerInfo serverInfo =
        new ServerInfo(
            Protocol.UDS_PROTOCOL,
            Language.JAVA,
            MINIMUM_NUMAFLOW_VERSION.get(containerType),
            serverInfoAccessor.getSDKVersion(),
            metaData);
    log.info("Writing server info {} to {}", serverInfo, infoFilePath);
    serverInfoAccessor.write(serverInfo, infoFilePath);
  }
}
