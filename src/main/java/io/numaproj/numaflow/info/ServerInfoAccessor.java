package io.numaproj.numaflow.info;

import java.io.IOException;

public interface ServerInfoAccessor {
    /**
     * Get runtime Java SDK version.
     */
    String getSDKVersion();

    /**
     * Delete filePath if it exists.
     * Write serverInfo to filePath in Json format.
     * Append {@link ServerInfoConstants#EOF} as a new line to indicate end of file.
     *
     * @param serverInfo server information POJO
     * @param filePath file path to write to
     *
     * @throws IOException any IO exceptions are thrown to the caller.
     */
    void write(ServerInfo serverInfo, String filePath) throws IOException;

    /**
     * Read from filePath to retrieve server information POJO.
     * This API is only used for unit tests.
     *
     * @param filePath file path to read from
     *
     * @return server information POJO
     *
     * @throws IOException any IO exceptions are thrown to the caller.
     */
    ServerInfo read(String filePath) throws IOException;
}
