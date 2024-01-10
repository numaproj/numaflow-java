package io.numaproj.numaflow.reducer;

import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * ActorResponse is to store the response from ReduceActors.
 */
@Getter
@AllArgsConstructor
class ActorResponse {
    // TODO - do we need keys? they seem already present in the ReduceResponse
    String[] keys;
    ReduceOuterClass.ReduceResponse response;
}
