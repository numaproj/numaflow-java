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
    // FIXME - with the latest proto update, each response has a single set of keys, hence we can remove the keys field here.
    String[] keys;
    ReduceOuterClass.ReduceResponse response;
}
