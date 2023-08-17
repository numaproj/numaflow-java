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
    String[] keys;
    ReduceOuterClass.ReduceResponse response;
}
