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
    public static final String MINIMUM_CLIENT_VERSION = ">= 1.2.0-0";
    @JsonProperty("protocol")
    private Protocol protocol;
    @JsonProperty("language")
    private Language language;
    @JsonProperty("minimum_client_version")
    private String minimum_client_version;
    @JsonProperty("version")
    private String version;
    @JsonProperty("metadata")
    private Map<String, String> metadata;
}
