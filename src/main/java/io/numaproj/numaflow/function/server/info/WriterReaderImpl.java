package io.numaproj.numaflow.function.server.info;

public class WriterReaderImpl implements WriterReader {
    @Override
    public String getSDKVersion() {
        // This only works for Java 9 and above.
        // Since we already use 11+ for numaflow SDK, it's safe to apply this approach.
        return String.valueOf(Runtime.version().version().get(0));
    }

    @Override
    public void write(ServerInfo serverInfo, String filePath) throws Exception {
        
    }

    @Override
    public ServerInfo read(String filePath) throws Exception {
        return null;
    }
}
