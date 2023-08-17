package io.numaproj.numaflow.shared;

import io.grpc.Context;
import io.grpc.Metadata;

public class Constants {
        public static final String DEFAULT_SOCKET_PATH = "/var/run/numaflow/function.sock";

        public static final int DEFAULT_MESSAGE_SIZE = 1024 * 1024 * 64;

        public static final String WIN_START_KEY = "x-numaflow-win-start-time";

        public static final String WIN_END_KEY = "x-numaflow-win-end-time";

        public static final String EOF = "EOF";

        public static final String SUCCESS = "SUCCESS";

        public static final String DELIMITTER = ":";

        public static final Metadata.Key<String> DATUM_METADATA_WIN_START = Metadata.Key.of(
                        Constants.WIN_START_KEY,
                        Metadata.ASCII_STRING_MARSHALLER);

        public static final Metadata.Key<String> DATUM_METADATA_WIN_END = Metadata.Key.of(
                        Constants.WIN_END_KEY,
                        Metadata.ASCII_STRING_MARSHALLER);
        public static final Context.Key<String> WINDOW_START_TIME = Context.keyWithDefault(
                        Constants.WIN_START_KEY,
                        "");

        public static final Context.Key<String> WINDOW_END_TIME = Context.keyWithDefault(
                        Constants.WIN_END_KEY,
                        "");
}
