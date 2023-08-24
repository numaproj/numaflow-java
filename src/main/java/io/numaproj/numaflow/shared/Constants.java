package io.numaproj.numaflow.shared;

import io.grpc.Context;
import io.grpc.Metadata;

public class Constants {
        public static final String MAP_SOCKET_PATH = "/var/run/numaflow/map.sock";

        public static final String REDUCE_SOCKET_PATH = "/var/run/numaflow/reduce.sock";

        public static final String MAPSTREAM_SOCKET_PATH = "/var/run/numaflow/mapstream.sock";

        public static final String SINK_SOCKET_PATH = "/var/run/numaflow/sink.sock";

        public static final String SOURCE_SOCKET_PATH = "/var/run/numaflow/source.sock";

    public static final String SIDE_INPUT_SOCKET_PATH = "/var/run/numaflow/sideinput.sock";

        public static final String SOURCE_TRANSFORMER_SOCKET_PATH = "/var/run/numaflow/sourcetransform.sock";

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
    public static final String DEFAULT_SERVER_INFO_FILE_PATH = "/var/run/numaflow/server-info";

    public static final String INFO_EOF = "U+005C__END__";
}
