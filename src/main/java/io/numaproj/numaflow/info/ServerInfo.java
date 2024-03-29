package io.numaproj.numaflow.info;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

/**
 * Server Information to be used by client to determine:
 * - protocol: what is right protocol to use (UDS or TCP)
 * - language: what is language used by the server
 * - version: what is the numaflow sdk version used by the server
 * - metadata: other information
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ServerInfo {
    @JsonProperty("protocol")
    private Protocol protocol;
    @JsonProperty("language")
    private Language language;
    @JsonProperty("version")
    private String version;
    @JsonProperty("metadata")
    private Map<String, String> metadata;
}
