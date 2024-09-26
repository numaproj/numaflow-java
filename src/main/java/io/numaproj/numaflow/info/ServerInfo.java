package io.numaproj.numaflow.info;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

import static java.util.Map.entry;

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
    public static final Map<ContainerType, String> MINIMUM_NUMAFLOW_VERSION = Map.ofEntries(
            entry(ContainerType.Sourcer, "1.3.1-z"),
            entry(ContainerType.Sourcetransformer, "1.3.1-z"),
            entry(ContainerType.Sinker, "1.3.1-z"),
            entry(ContainerType.Mapper, "1.3.1-z"),
            entry(ContainerType.Reducer, "1.3.1-z"),
            entry(ContainerType.Reducestreamer, "1.3.1-z"),
            entry(ContainerType.Sessionreducer, "1.3.1-z"),
            entry(ContainerType.Sideinput, "1.3.1-z"),
            entry(ContainerType.Fbsinker, "1.3.1-z"),
            entry(ContainerType.Unknown, "1.3.1-z")
    );
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
