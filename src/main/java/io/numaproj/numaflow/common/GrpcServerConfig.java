package io.numaproj.numaflow.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class GrpcServerConfig {

    private String socketPath;
    private int maxMessageSize;
}
