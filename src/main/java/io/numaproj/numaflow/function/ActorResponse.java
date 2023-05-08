package io.numaproj.numaflow.function;

import io.numaproj.numaflow.function.v1.Udfunction;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * ActorResponse is to store the response from ReduceActors.
 */
@Getter
@AllArgsConstructor
class ActorResponse {
    String[] keys;
    Udfunction.DatumResponseList datumList;
}
