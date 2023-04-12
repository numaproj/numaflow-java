package io.numaproj.numaflow.function.server.info;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
// TODO - doc
public class ServerInfo {
    @JsonProperty("protocol")
    private String protocol;
    @JsonProperty("language")
    private String language;
    @JsonProperty("version")
    private String version;
    @JsonProperty("metadata")
    private Map<String, String> metadata;
}
