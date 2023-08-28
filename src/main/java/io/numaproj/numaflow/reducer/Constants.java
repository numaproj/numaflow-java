package io.numaproj.numaflow.reducer;

import io.grpc.Context;

class Constants {
    public static final int DEFAULT_MESSAGE_SIZE = 1024 * 1024 * 64;

    public static final String SOCKET_PATH = "/var/run/numaflow/reduce.sock";

    public static final String WIN_START_KEY = "x-numaflow-win-start-time";

    public static final String WIN_END_KEY = "x-numaflow-win-end-time";

    public static final Context.Key<String> WINDOW_START_TIME = Context.keyWithDefault(
            WIN_START_KEY,
                    "");

    public static final Context.Key<String> WINDOW_END_TIME = Context.keyWithDefault(
            WIN_END_KEY,
            "");

    public static final String EOF = "EOF";

    public static final String SUCCESS = "SUCCESS";

    public static final String DELIMITER = ":";
}
