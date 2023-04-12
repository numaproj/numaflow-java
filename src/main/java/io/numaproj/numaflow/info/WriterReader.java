package io.numaproj.numaflow.info;

import java.io.IOException;

// TODO - doc
public interface WriterReader {
    String getSDKVersion();

    void write(ServerInfo serverInfo, String filePath) throws IOException;

    ServerInfo read(String filePath) throws IOException;
}
