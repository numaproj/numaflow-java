package io.numaproj.numaflow.sinker;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

/**
 * ResponseList is used to return the list of responses from udsink
 */

@Getter
@Builder(builderMethodName = "newBuilder")
public class ResponseList {
    @Singular("addResponse")
    private Iterable<Response> responses;
}
