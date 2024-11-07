package io.numaproj.numaflow.shared;

// Currently each of the UDFs (Mapper, BatchMapper, Sourcer) has its own GrpcConfig class.
// To start a gRPC server, we need to pass gRPC configurations to the GrpcServerWrapper.
// In order to make the GrpcServerWrapper more generic, we create this GrpcConfigRetriever interface,
// which is implemented by the UDFs' GrpcConfig classes.
public interface GrpcConfigRetriever {
    String getSocketPath();
    int getMaxMessageSize();
    String getInfoFilePath();
    int getPort();
    boolean isLocal();
}
