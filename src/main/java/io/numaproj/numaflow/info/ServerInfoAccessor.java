package io.numaproj.numaflow.info;

public interface ServerInfoAccessor {
    /**
     * Get current runtime numaflow-java SDK version.
     */
    String getSDKVersion();

    /**
     * Delete filePath if it exists.
     * Write serverInfo to filePath in Json format.
     *
     * @param serverInfo server information POJO
     * @param filePath file path to write to
     *
     * @throws Exception any exceptions are thrown to the caller.
     */
    void write(ServerInfo serverInfo, String filePath) throws Exception;

    /**
     * Read from filePath to retrieve server information POJO.
     * This API is only used for unit tests.
     *
     * @param filePath file path to read from
     *
     * @return server information POJO
     *
     * @throws Exception any exceptions are thrown to the caller.
     */
    ServerInfo read(String filePath) throws Exception;
}
