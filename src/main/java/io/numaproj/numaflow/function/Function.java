package io.numaproj.numaflow.function;

import io.grpc.Context;
import io.grpc.Metadata;

public class Function {
  public static final String SOCKET_PATH = "/var/run/numaflow/function.sock";

  public static final int DEFAULT_MESSAGE_SIZE = 1024 * 1024 * 4;
  public static final String DATUM_KEY = "x-numaflow-datum-key";
  public static final Metadata.Key<String> DATUM_METADATA_KEY = Metadata.Key.of(Function.DATUM_KEY, Metadata.ASCII_STRING_MARSHALLER);
  public static final Context.Key<String> DATUM_CONTEXT_KEY = Context.keyWithDefault(Function.DATUM_KEY, "");
}
