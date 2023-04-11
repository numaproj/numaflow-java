package io.numaproj.numaflow.function.server.info;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@AllArgsConstructor
@Getter
@Setter
// TODO - doc
public class ServerInfo {
    private String protocol;
    private String language;
    private String version;
    private Map<String, String> metadata;
}
