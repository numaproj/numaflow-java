package io.numaproj.numaflow.function.server.info;

// TODO - doc
public interface WriterReader {
    public String getSDKVersion();

    public void write(ServerInfo serverInfo, String filePath) throws Exception;

    public ServerInfo read(String filePath) throws Exception;
}
