package io.numaproj.numaflow.function.server.info;

import java.io.IOException;

// TODO - doc
public interface WriterReader {
    public String getSDKVersion();

    public void write(ServerInfo serverInfo, String filePath) throws IOException;

    public ServerInfo read(String filePath) throws IOException;
}
