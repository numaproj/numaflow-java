package io.numaproj.numaflow.shared;

public interface GrpcConfigRetriever {
    String getSocketPath();
    int getMaxMessageSize();
    String getInfoFilePath();
    int getPort();
    boolean isLocal();
}
