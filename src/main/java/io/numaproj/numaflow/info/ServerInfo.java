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
 * - minimum_numaflow_version: lower bound for the supported Numaflow version
 * - version: what is the numaflow sdk version used by the server
 * - metadata: other information
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ServerInfo {
    // Specify the minimum Numaflow version required by the current SDK version
    // To update this value, please follow the instructions for MINIMUM_NUMAFLOW_VERSION in
    // https://github.com/numaproj/numaflow-rs/blob/main/src/shared.rs
    public static final String MINIMUM_NUMAFLOW_VERSION = "1.3.1-z";
    @JsonProperty("protocol")
    private Protocol protocol;
    @JsonProperty("language")
    private Language language;
    @JsonProperty("minimum_numaflow_version")
    private String minimum_numaflow_version;
    @JsonProperty("version")
    private String version;
    @JsonProperty("metadata")
    private Map<String, String> metadata;
}
